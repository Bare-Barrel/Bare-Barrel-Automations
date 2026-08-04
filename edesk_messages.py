"""
eDesk API client: collect full message details for every message ID
referenced by edesk_tickets in BigQuery, and load them into their own
BigQuery table (edesk_messages).

A message is treated as immutable once fetched: once we have its details,
we don't re-fetch it. This pipeline does a straight set difference:
every message id referenced across edesk_tickets, minus every message id
already present in edesk_messages, leaves the ids that haven't been processed yet. 
Only those get fetched from the API and appended.

This pipeline must run after edesk_tickets.py has loaded a fresh
edesk_tickets table.

Docs used:
- Read Message: https://developers.edesk.com/reference/getmessage.md
    GET https://api.edesk.com/v1/messages/{messageId}
- Auth / rate limit: see edesk_client.py's module docstring.
"""

from __future__ import annotations
import json
import logging
from typing import Any, Dict, Iterable, Iterator, List
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
import logger_setup
import bigquery_utils
from edesk_client import EDeskClient, EDeskAPIError


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open("config.json") as f:
    config = json.load(f)

PROJECT_ID = "modern-sublime-383117"
SOURCE_DATASET = "edesk_api"
SOURCE_TABLE = "edesk_tickets"
DEST_DATASET = "edesk_api"
DEST_TABLE = "edesk_messages"


class EDeskMessagesClient(EDeskClient):
    """EDeskClient plus message-reading methods."""

    # ---- Read Message ----------------------------------------------------
    # https://developers.edesk.com/reference/getmessage.md
    def get_message(self, message_id: int) -> Dict[str, Any]:
        return self._request("GET", f"/messages/{message_id}")

    def get_message_details(self, message_ids: Iterable[int]) -> Iterator[Dict[str, Any]]:
        """Fetch full details for each message id in `message_ids` (rate-limit aware)."""
        for message_id in message_ids:
            if message_id is None:
                continue
            try:
                logger.info("Getting details for message: %s", message_id)
                yield self.get_message(message_id)
            except EDeskAPIError as exc:
                logger.error("Failed to fetch message %s: %s", message_id, exc)


def get_ticket_referenced_message_ids() -> set[int]:
    """
    Gets every unique message id referenced across all tickets currently in
    BigQuery, read from the `messages_ids` column of edesk_tickets.
    """
    client = bigquery.Client(project=PROJECT_ID)

    sql = f"""
        SELECT messages_ids
        FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
        WHERE messages_ids IS NOT NULL
    """
    query_job = client.query(sql)
    df = query_job.result().to_dataframe()

    ids: set[int] = set()
    for raw in df["messages_ids"]:
        if not raw:
            continue
        try:
            ids.update(json.loads(raw))
        except (TypeError, json.JSONDecodeError):
            logger.warning("Couldn't parse messages_ids value: %r", raw)

    logger.info("Found %d unique message ids referenced across %d ticket rows", len(ids), len(df))
    return ids


def get_already_fetched_message_ids() -> set[int]:
    """
    Gets every message id already present in edesk_messages. Returns an empty set
    if the table doesn't exist yet (first run, triggers a full historical pull).
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"

    try:
        client.get_table(table_id)
    except NotFound:
        logger.info("%s doesn't exist yet -- treating all referenced ids as new.", table_id)
        return set()

    sql = f"SELECT DISTINCT id FROM `{table_id}`"
    rows = client.query(sql).result()
    ids = {row.id for row in rows}
    logger.info("%d message ids already present in %s", len(ids), table_id)
    return ids


def get_new_message_ids() -> List[int]:
    """
    Message ids referenced by tickets that haven't been fetched into
    edesk_messages yet: the set difference between every id referenced
    across edesk_tickets and every id already in edesk_messages.
    """
    referenced_ids = get_ticket_referenced_message_ids()
    already_fetched_ids = get_already_fetched_message_ids()
    new_ids = referenced_ids - already_fetched_ids

    logger.info(
        "%d referenced, %d already fetched, %d new",
        len(referenced_ids), len(already_fetched_ids), len(new_ids),
    )
    return sorted(new_ids)


def parse_messages_json(raw_messages) -> pd.DataFrame:
    """
    Args:
        raw_messages: list of message dicts as returned by
            GET /messages/{messageId}, or a path to a JSON file
            containing that list.

    Returns:
        A flat pandas DataFrame, one row per message, ready for BigQuery.
    """
    # Nested/variable-shape fields -> stored as JSON strings, same
    # rationale as edesk_tickets.parse_tickets_json.
    JSON_COLUMNS = ["from_user", "attachments", "errors"]
    TIMESTAMP_COLUMNS = ["created_at"]

    if isinstance(raw_messages, str):
        with open(raw_messages) as f:
            raw_messages = json.load(f)

    messages = [m["data"] if isinstance(m, dict) and "data" in m else m for m in raw_messages]
    df = pd.DataFrame(messages)

    # Parse timestamps into real datetimes. Tolerant of either representation
    # documented/observed for these fields (formatted string or numeric epoch).
    # eDesk's timestamps are UTC (confirmed by cross-referencing a ticket's
    # API last_updated_at against its dashboard display).
    def _parse_timestamp(v: Any) -> pd.Timestamp:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return pd.to_datetime(v, unit="s", utc=True, errors="coerce")
        return pd.to_datetime(v, utc=True, errors="coerce")

    for col in TIMESTAMP_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_parse_timestamp)

    for col in JSON_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v) if v is not None else None)

    return df


def update_data() -> None:
    api_token = config["edesk_api_key"]
    client = EDeskMessagesClient(api_token=api_token)

    message_ids = get_new_message_ids()
    if not message_ids:
        logger.info("No new message ids to fetch. Skipping execution.")
        return

    messages = list(client.get_message_details(message_ids))
    logger.info("Fetched details for %d messages", len(messages))

    json_path = "edesk_messages_detailed.json"
    with open(json_path, "w") as f:
        json.dump(messages, f, indent=2, default=str)
    logger.info("Wrote %s", json_path)

    df = parse_messages_json(json_path)
    logger.info("Parsed %d messages from %s", len(df), json_path)

    df["loaded_at"] = pd.Timestamp.now(tz="UTC")

    # Always append: get_new_message_ids already only returns ids not yet
    # in the table (including the full historical set on a first run
    # when the table doesn't exist yet).
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(df, table_id, PROJECT_ID, "append")


if __name__ == "__main__":
    update_data()
