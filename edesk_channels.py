"""
eDesk API client: collect all channels and load them into BigQuery.

Docs used:
- List Channels: https://developers.edesk.com/reference/listchannels.md
    GET https://api.edesk.com/v1/channels
- Auth / rate limit: see edesk_client.py's module docstring.

Channels are a small reference/dimension table. This pipeline always does a full
`"replace"` load.
"""

from __future__ import annotations
import json
import logging
from typing import Any, Dict, Iterator, List
import pandas as pd
import logger_setup
import bigquery_utils
from edesk_client import EDeskClient, EDeskAPIError


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open("config.json") as f:
    config = json.load(f)

PROJECT_ID = "modern-sublime-383117"
DEST_DATASET = "edesk_api"
DEST_TABLE = "edesk_channels"


class EDeskChannelsClient(EDeskClient):
    """EDeskClient plus channel-reading methods."""

    # ---- List Channels ---------------------------------------------------
    # https://developers.edesk.com/reference/listchannels.md
    def list_channels_page(self, page: int = 1, items_per_page: int = 100) -> Dict[str, Any]:
        """
        Single raw call to GET /channels. Not documented on the reference
        page, but confirmed via a real response to return the same
        {"data": [...], "paginator": {...}} shape as List Tickets.
        """
        params = {"page": page, "itemsPerPage": items_per_page}
        data = self._request("GET", "/channels", params=params)
        if not isinstance(data, dict) or "data" not in data:
            raise EDeskAPIError(f"Unexpected /channels response shape: {data!r}")
        return data

    def iter_channel_pages(self, items_per_page: int = 100, max_pages: int = 1000) -> Iterator[Dict[str, Any]]:
        """Yield each raw page envelope (not individual channels) until exhausted."""
        page_num = 1
        total_seen = 0

        while True:
            if page_num > max_pages:
                logger.warning("Hit max_pages=%d safety cap, stopping.", max_pages)
                return

            resp = self.list_channels_page(page=page_num, items_per_page=items_per_page)
            yield resp

            channels = resp.get("data") or []
            paginator = resp.get("paginator") or {}
            total_items_count = paginator.get("totalItemsCount")
            total_seen += len(channels)

            if not channels:
                return
            if total_items_count is not None and total_seen >= total_items_count:
                return
            if total_items_count is None and len(channels) < items_per_page:
                return

            page_num += 1

    def iter_all_channels(self, items_per_page: int = 100) -> Iterator[Dict[str, Any]]:
        """Yield every channel, paging through iter_channel_pages under the hood."""
        for page in self.iter_channel_pages(items_per_page=items_per_page):
            yield from (page.get("data") or [])


def parse_channels_json(raw_channels) -> pd.DataFrame:
    """
    Args:
        raw_channels: a flat list of channel dicts (e.g. from
            list(client.iter_all_channels())), or a path to a JSON file
            containing that list.

    Returns:
        A flat pandas DataFrame, one row per channel, ready for BigQuery.
    """
    if isinstance(raw_channels, str):
        with open(raw_channels) as f:
            raw_channels = json.load(f)

    channels = [c["data"] if isinstance(c, dict) and "data" in c else c for c in raw_channels]
    df = pd.DataFrame(channels)
    return df


def update_data() -> None:
    api_token = config["edesk_api_key"]
    client = EDeskChannelsClient(api_token=api_token)

    raw_pages = list(client.iter_channel_pages())
    json_path = "edesk_channels.json"
    with open(json_path, "w") as f:
        json.dump(raw_pages, f, indent=2, default=str)
    logger.info("Wrote %d raw page(s) to %s", len(raw_pages), json_path)

    channels: List[Dict[str, Any]] = []
    for page in raw_pages:
        channels.extend(page.get("data") or [])
    logger.info("Collected %d channels", len(channels))

    df = parse_channels_json(channels)
    df["loaded_at"] = pd.Timestamp.now(tz="UTC")
    logger.info("Parsed %d channels", len(df))

    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(df, table_id, PROJECT_ID, "append")


if __name__ == "__main__":
    update_data()
