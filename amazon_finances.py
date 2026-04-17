from sp_api.base import Marketplaces
from sp_api.api import Finances
from sp_api.util import throttle_retry, load_all_pages
from sp_api.base.exceptions import SellingApiRequestThrottledException, SellingApiServerException
import datetime as dt
import time
import pandas as pd
import logging
import logger_setup
from utility import to_list
import bigquery_utils


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

TENANTS = bigquery_utils.get_tenants()

PROJECT_ID = "modern-sublime-383117"
DEST_DATASET = "finances"
DEST_TABLE = "financial_events"


@throttle_retry()
@load_all_pages()
def load_all_financial_events(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    try:
        response = Finances(
            account=f'{account}-{marketplace}',
            marketplace=Marketplaces[marketplace]
            ).list_financial_events(**kwargs)
    except (SellingApiRequestThrottledException, SellingApiServerException) as error:
        logger.warning(error)

    return response


def get_financial_events(account='Bare Barrel', 
                          marketplace='US',
                          **kwargs
                          ):
    """
    Gets a list of financial events from a posted_after to a posted_before date.
    """
    df = pd.DataFrame()

    logger.info(f'Retrieving Financial Events from {account}-{marketplace} \n {kwargs}')
    response = load_all_financial_events(account, marketplace, **kwargs)

    page_no = 0
    financial_events_data = pd.DataFrame()
    for page in response:
        page_no += 1
        logger.info(f"\tProcessing Page {page_no}. . .")
        payload = page.payload.get('FinancialEvents').get('ShipmentEventList')
        df = pd.json_normalize(payload, sep="_")

        if not df.empty and "ShipmentItemList" in df.columns:
            df_items = df.explode("ShipmentItemList", ignore_index=True)
            df_items = df_items.join(pd.json_normalize(df_items["ShipmentItemList"]))
            df_items = df_items.drop(columns=["ShipmentItemList"])
            
            df_items["marketplace"] = marketplace
            df_items["tenant_id"] = TENANTS[account]
            df_items["created_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            financial_events_data = pd.concat([financial_events_data, df_items], ignore_index=True)
            time.sleep(0.5)
    
    return financial_events_data


if __name__ == '__main__':
    marketplaces = ['US', 'CA', 'UK']
    lookback_180 = 180 # lookback window for daily
    lookback_730 = 730 # lookback window for weekly

    # Use this for backfilling
    # posted_after = dt.datetime(2025, 11, 1, 0, 0, 0, tzinfo=dt.timezone.utc).isoformat()
    # posted_before = dt.datetime(2025, 12, 1, 0, 0, 0, tzinfo=dt.timezone.utc).isoformat()

    date_today = dt.datetime.now(dt.timezone.utc).date()
    weekday_num = dt.datetime.today().weekday()
    hour = dt.datetime.now().hour

    for account in TENANTS.keys():
        for marketplace in to_list(marketplaces):
            if weekday_num == 6 and hour == 5: # sunday at 5am
                posted_after = date_today - dt.timedelta(days=lookback_730)
                posted_before = date_today

                financial_events_items = get_financial_events(
                    account,
                    marketplace,
                    MarketplaceId = Marketplaces[marketplace].value,
                    PostedAfter = posted_after.isoformat(),
                    PostedBefore = posted_before.isoformat(),
                    MaxResultsPerPage = 100
                    )
                
                # Load data to BigQuery
                table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
                bigquery_utils.load_to_bigquery(financial_events_items, table_id, PROJECT_ID, "append")

            else: # daily at 3am
                posted_after = date_today - dt.timedelta(days=lookback_180)
                posted_before = date_today

                financial_events_items = get_financial_events(
                    account,
                    marketplace,
                    MarketplaceId = Marketplaces[marketplace].value,
                    PostedAfter = posted_after.isoformat(),
                    PostedBefore = posted_before.isoformat(),
                    MaxResultsPerPage = 100
                    )
                
                # Load data to BigQuery
                table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
                bigquery_utils.load_to_bigquery(financial_events_items, table_id, PROJECT_ID, "append")