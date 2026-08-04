"""
eDesk API client: collect tickets and their details, load to BigQuery.

Docs used:
- List Tickets:  https://developers.edesk.com/reference/listtickets.md
    GET https://api.edesk.com/v1/tickets
- Read Ticket:   https://developers.edesk.com/reference/getticket.md
    GET https://api.edesk.com/v1/tickets/{ticketId}
- Pagination:    https://developers.edesk.com/reference/pagination.md
- Auth / rate limit: see edesk_client.py's module docstring -- shared
  across every eDesk pipeline.

Pagination:
List responses use real page-based pagination: pass `page` and
`itemsPerPage` query params, and every list response includes a
`paginator` object:

    {
      "data": [...],
      "paginator": {"currentPage": 2, "itemsPerPage": 10, "totalItemsCount": 86}
    }

`iter_all_tickets` uses this.

Known docs/reality mismatch:
The OpenAPI schema types `Ticket.created_at` as `number` (implying a raw
epoch), but a real response from this account returned it as a
formatted string (e.g. "2025-03-07 08:40:44"), matching how
`last_updated_at` is documented. `parse_tickets_json` below parses both
`created_at` and `last_updated_at` through `pd.to_datetime`, which
handles either representation.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, Iterable, Iterator, Optional
import json
import pandas as pd
import logger_setup
import bigquery_utils
from edesk_client import EDeskClient, EDeskAPIError
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open("config.json") as f:
    config = json.load(f)

PROJECT_ID = "modern-sublime-383117"
DEST_DATASET = "edesk_api"
DEST_TABLE = "edesk_tickets"


class EDeskTicketsClient(EDeskClient):
    """EDeskClient plus ticket-reading methods."""

    # ---- List Tickets --------------------------------------------------
    # https://developers.edesk.com/reference/listtickets.md
    def list_tickets_page(
        self,
        page: int = 1,
        items_per_page: int = 100,
        order_by: str = "id",
        order_direction: str = "asc",
        filter_contact_id_equals: Optional[int] = None,
        filter_channel_id_equals: Optional[int] = None,
        filter_status_equals: Optional[str] = None,
        filter_type_equals: Optional[str] = None,
        filter_sales_order_id_equals: Optional[int] = None,
        filter_created_at_gte: Optional[int] = None,
        filter_created_at_lte: Optional[int] = None,
        filter_last_updated_at_gte: Optional[str] = None,
        filter_last_updated_at_lte: Optional[str] = None,
        filter_owner_user_id_equals: Optional[int] = None,
        filter_seller_order_id_equals: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Single raw call to GET /tickets. Returns the full response dict:
        {"data": [tickets...], "paginator": {"currentPage", "itemsPerPage",
        "totalItemsCount"}}.
        """
        params = {
            "page": page,
            "itemsPerPage": items_per_page,
            "order_by": order_by,
            "order_direction": order_direction,
            "filter_contact_id_equals": filter_contact_id_equals,
            "filter_channel_id_equals": filter_channel_id_equals,
            "filter_status_equals": filter_status_equals,
            "filter_type_equals": filter_type_equals,
            "filter_sales_order_id_equals": filter_sales_order_id_equals,
            "filter_created_at_gte": filter_created_at_gte,
            "filter_created_at_lte": filter_created_at_lte,
            "filter_last_updated_at_gte": filter_last_updated_at_gte,
            "filter_last_updated_at_lte": filter_last_updated_at_lte,
            "filter_owner_user_id_equals": filter_owner_user_id_equals,
            "filter_seller_order_id_equals": filter_seller_order_id_equals,
        }
        data = self._request("GET", "/tickets", params=params)
        if not isinstance(data, dict) or "data" not in data:
            raise EDeskAPIError(f"Unexpected /tickets response shape: {data!r}")
        return data

    def iter_all_tickets(
        self,
        items_per_page: int = 100,
        max_pages: int = 100_000,
        **extra_filters: Any,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yield every ticket matching the given filters, paging through the
        documented `page` / `itemsPerPage` / `paginator` mechanism (see
        module docstring). `extra_filters` accepts any of the
        `filter_*` / `order_by` / `order_direction` kwargs that
        `list_tickets_page` takes.
        """
        page_num = 1
        total_seen = 0

        while True:
            if page_num > max_pages:
                logger.warning("Hit max_pages=%d safety cap, stopping.", max_pages)
                return

            resp = self.list_tickets_page(page=page_num, items_per_page=items_per_page, **extra_filters)
            tickets = resp.get("data") or []
            paginator = resp.get("paginator") or {}
            total_items_count = paginator.get("totalItemsCount")

            if not tickets:
                logger.info("Page %d: empty, done. Total tickets collected: %d", page_num, total_seen)
                return

            for ticket in tickets:
                total_seen += 1
                yield ticket

            logger.info(
                "Page %d: %d tickets (paginator: currentPage=%s itemsPerPage=%s totalItemsCount=%s), "
                "total collected so far: %d",
                page_num, len(tickets), paginator.get("currentPage"),
                paginator.get("itemsPerPage"), total_items_count, total_seen,
            )

            if total_items_count is not None and total_seen >= total_items_count:
                return
            if total_items_count is None and len(tickets) < items_per_page:
                # No paginator info to trust -- fall back to the standard
                # "short page means last page" heuristic.
                return

            page_num += 1

    # ---- Read Ticket -----------------------------------------------------
    # https://developers.edesk.com/reference/getticket.md
    def get_ticket(self, ticket_id: int) -> Dict[str, Any]:
        return self._request("GET", f"/tickets/{ticket_id}")

    def get_ticket_details(self, tickets: Iterable[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """Fetch full details for each ticket in `tickets` (rate-limit aware)."""
        for ticket in tickets:
            ticket_id = ticket.get("id")
            if ticket_id is None:
                logger.warning("Skipping ticket with no id: %r", ticket)
                continue
            try:
                logger.info("Getting details for ticket: %s", ticket_id)
                yield self.get_ticket(ticket_id)
            except EDeskAPIError as exc:
                logger.error("Failed to fetch ticket %s: %s", ticket_id, exc)


def _flatten_sales_order(sales_order: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Expand a ticket's nested `sales_order` object into individual
    `sales_order_*` columns instead of a single JSON blob.

    Note: `sales_order.id` is intentionally dropped because it duplicates
    the ticket-level `sales_order_id` field.
    """
    so = sales_order if isinstance(sales_order, dict) else {}
    delivery_dates = so.get("sales_order_delivery_dates") or {}

    def _json_or_none(v: Any) -> Optional[str]:
        return json.dumps(v) if v is not None else None

    return {
        "sales_order_channel_id": so.get("channel_id"),
        "sales_order_status": so.get("status"),
        "sales_order_seller_order_id": so.get("seller_order_id"),
        "sales_order_created_at": so.get("created_at"),
        "sales_order_contact_id": so.get("contact_id"),
        "sales_order_total_amount": so.get("total_amount"),
        "sales_order_shipping_amount": so.get("shipping_amount"),
        "sales_order_last_updated_at": so.get("last_updated_at"),
        "sales_order_order_shipped_at": so.get("order_shipped_at"),
        "sales_order_ticket_id": so.get("ticket_id"),
        "sales_order_order_notes_id": so.get("order_notes_id"),
        "sales_order_expected_delivery_from": delivery_dates.get("expected_delivery_from"),
        "sales_order_expected_delivery_to": delivery_dates.get("expected_delivery_to"),
        "sales_order_tracking_codes": _json_or_none(so.get("tracking_codes")),
        "sales_order_order_items": _json_or_none(so.get("order_items")),
        "sales_order_ship_to": _json_or_none(so.get("ship_to")),
        "sales_order_bill_to": _json_or_none(so.get("bill_to")),
        "sales_order_custom_fields": _json_or_none(so.get("custom_fields")),
    }


def parse_tickets_json(raw_tickets) -> pd.DataFrame:
    """
    Args:
        raw_tickets: either the parsed JSON (list of {"data": {...}} dicts,
            matching edesk_tickets_detailed.json) or a path to that file.

    Returns:
        A flat pandas DataFrame, one row per ticket, ready for BigQuery.
    """
    # Columns that are nested objects -> stored as JSON strings.
    JSON_OBJECT_COLUMNS = ["custom_fields"]

    # Columns that are arrays -> stored as JSON strings (see docstring above).
    JSON_ARRAY_COLUMNS = ["tags_ids", "messages_ids"]

    TIMESTAMP_COLUMNS = [
        "created_at",
        "last_updated_at",
        "sales_order_created_at",
        "sales_order_last_updated_at",
        "sales_order_order_shipped_at",
    ]

    if isinstance(raw_tickets, str):
        with open(raw_tickets) as f:
            raw_tickets = json.load(f)

    # Unwrap the {"data": {...}} envelope returned by the Read Ticket endpoint.
    tickets = [item["data"] if "data" in item else item for item in raw_tickets]
    df = pd.DataFrame(tickets)

    # Expand the nested `sales_order` object into its own `sales_order_*` columns
    if "sales_order" in df.columns:
        sales_order_df = pd.DataFrame(
            df["sales_order"].apply(_flatten_sales_order).tolist(), index=df.index
        )
        df = pd.concat([df.drop(columns=["sales_order"]), sales_order_df], axis=1)

    # `time_left_to_reply` is inconsistent in the API response: it's an int
    # (seconds, can be negative) for most tickets but `false` for some.
    # Normalize the boolean case to a null rather than 0/1.
    if "time_left_to_reply" in df.columns:
        df["time_left_to_reply"] = df["time_left_to_reply"].apply(
            lambda v: v if isinstance(v, (int, float)) and not isinstance(v, bool) else None
        ).astype("Int64")

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

    # Serialize nested objects/arrays to JSON strings so pandas_gbq loads
    # them as plain STRING columns instead of failing on schema inference.
    for col in JSON_OBJECT_COLUMNS + JSON_ARRAY_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v) if v is not None else None)

    return df


def get_max_last_updated_date() -> Optional[str]:
    """
    Return the date (YYYY-MM-DD) to use for filter_last_updated_at_gte on
    the next fetch: the UTC calendar date of the latest last_updated_at
    already in BigQuery.

    filter_last_updated_at_gte is date-only and inclusive-start-of-day in
    the eDesk account's timezone (confirmed UTC+02:00, ahead of UTC).

    Returns None if the table doesn't exist yet, or exists but is empty,
    signaling a full historical pull.
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"

    try:
        client.get_table(table_id)
    except NotFound:
        logger.info("%s doesn't exist yet. Doing a full historical pull.", table_id)
        return None

    sql = f"SELECT MAX(last_updated_at) AS max_ts FROM `{table_id}`"
    row = list(client.query(sql).result())[0]
    if row.max_ts is None:
        logger.info("%s is empty. Doing a full historical pull.", table_id)
        return None

    max_last_updated_date = row.max_ts.date().isoformat()
    logger.info("Using filter_last_updated_at_gte=%s", max_last_updated_date)
    return max_last_updated_date


def update_data() -> None:
    api_token = config["edesk_api_key"]

    client = EDeskTicketsClient(api_token=api_token)

    max_last_updated_date = get_max_last_updated_date()
    is_incremental = max_last_updated_date is not None

    # 1. Collect the list of tickets (optionally add filters, e.g. filter_status_equals="Open").
    tickets = list(client.iter_all_tickets(filter_last_updated_at_gte=max_last_updated_date))
    logger.info(
        "Collected %d tickets%s", len(tickets),
        f" updated since {max_last_updated_date}" if is_incremental else " (full historical pull)",
    )

    # 2. Fetch full details for each ticket.
    detailed_tickets = list(client.get_ticket_details(tickets))
    logger.info("Fetched details for %d tickets", len(detailed_tickets))

    json_path = "edesk_tickets_detailed.json"
    with open(json_path, "w") as f:
        json.dump(detailed_tickets, f, indent=2, default=str)
    logger.info("Wrote %s", json_path)

    # 3. Parse tickets in json to dataframe
    df = parse_tickets_json(json_path)
    logger.info("Parsed %d tickets from %s", len(df), json_path)

    df["loaded_at"] = pd.Timestamp.now(tz="UTC")

    # 4. Load data to BigQuery. First run replaces the table entirely;
    # every subsequent (incremental) run appends. Duplicate ticket rows
    # across runs are expected when a ticket is updated more than once,
    # and should be resolved downstream (e.g., a view that keeps only the
    # latest loaded_at per ticket id).
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(df, table_id, PROJECT_ID, "append" if is_incremental else "replace")


if __name__ == "__main__":
    update_data()
