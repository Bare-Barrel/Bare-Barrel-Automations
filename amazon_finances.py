import argparse
import datetime as dt
import logging
import time

import pandas as pd
from sp_api.api import Finances
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiServerException
from sp_api.util import load_all_pages, throttle_retry

import bigquery_utils
import logger_setup
from utility import to_list

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

TENANTS = bigquery_utils.get_tenants()
MARKETPLACES = ['US', 'CA', 'UK']

PROJECT_ID = "modern-sublime-383117"
DEST_DATASET = "finances"
DEST_TABLE = "financial_events"

# Mode is set by the scheduler, not by self-detecting day/hour.
#   daily   -- 180d lookback, 1 API call. Amazon's own per-call max.
#   monthly -- 365d lookback (chunked). Covers 99.93% of order corrections
#              per BigQuery analysis; 730d added almost nothing (<0.01%).
#
# Scheduler:
#   0 3 * * *     python get_financial_events.py --mode daily
#   0 5 1 * *     python get_financial_events.py --mode monthly
LOOKBACK_CONFIG = {
    "daily":   {"lookback_days": 180, "chunk_days": 180},
    "monthly": {"lookback_days": 365, "chunk_days": 180},
}


@throttle_retry(exception_classes=(SellingApiServerException,))
@load_all_pages()
def load_all_financial_events(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken

    throttle_retry() alone only retries 429s; SellingApiServerException (500)
    is added explicitly above. 400s are left uncaught, retrying won't fix them.
    """
    response = Finances(
        account=f'{account}-{marketplace}',
        marketplace=Marketplaces[marketplace]
        ).list_financial_events(**kwargs)
    return response


def get_financial_events(account='Bare Barrel',
                          marketplace='US',
                          **kwargs
                          ):
    """
    Fetches financial events between posted_after and posted_before.
    """
    logger.info(f'Retrieving Financial Events from {account}-{marketplace} \n {kwargs}')
    response = load_all_financial_events(account, marketplace, **kwargs)
 
    financial_events_data = pd.DataFrame()
    for page_no, page in enumerate(response, start=1):
        logger.info(f"\tProcessing Page {page_no}. . .")
        payload = page.payload.get('FinancialEvents').get('ShipmentEventList')
        df = pd.json_normalize(payload, sep="_")
 
        if not df.empty and "ShipmentItemList" in df.columns:
            df_items = df.explode("ShipmentItemList", ignore_index=True)
            df_items = df_items.join(pd.json_normalize(df_items["ShipmentItemList"]))
            df_items = df_items.drop(columns=["ShipmentItemList"])
 
            df_items["marketplace"] = marketplace
            df_items["tenant_id"] = TENANTS[account]
            financial_events_data = pd.concat([financial_events_data, df_items], ignore_index=True)
            time.sleep(0.5)
 
    return financial_events_data


def daterange_chunks(start_date, end_date, max_days=180):
    """
    Yield (chunk_start, chunk_end) tuples of at most max_days each.
    """
    current = start_date
    while current < end_date:
        chunk_end = min(current + dt.timedelta(days=max_days), end_date)
        yield current, chunk_end
        current = chunk_end


def update_data(mode):
    config = LOOKBACK_CONFIG[mode]
    date_today = dt.datetime.now(dt.timezone.utc).date()
    posted_after = date_today - dt.timedelta(days=config["lookback_days"])
    posted_before = date_today
 
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
 
    all_financial_events = pd.DataFrame()
 
    for account in TENANTS:
        for marketplace in to_list(MARKETPLACES):
            for chunk_start, chunk_end in daterange_chunks(posted_after, posted_before, max_days=config["chunk_days"]):
                chunk_df = get_financial_events(
                    account,
                    marketplace,
                    MarketplaceId=Marketplaces[marketplace].value,
                    PostedAfter=chunk_start.isoformat(),
                    PostedBefore=chunk_end.isoformat(),
                    MaxResultsPerPage=100
                )
                all_financial_events = pd.concat([all_financial_events, chunk_df], ignore_index=True)
 
    if not all_financial_events.empty:
        all_financial_events["loaded_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        bigquery_utils.load_to_bigquery(all_financial_events, table_id, PROJECT_ID, "append")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Collect Amazon financial events into BigQuery.")
    parser.add_argument(
        "--mode",
        choices=LOOKBACK_CONFIG.keys(),
        required=True,
        help="daily = 180d lookback. monthly = 365d reconciliation pass."
    )
    args = parser.parse_args()
 
    update_data(args.mode)