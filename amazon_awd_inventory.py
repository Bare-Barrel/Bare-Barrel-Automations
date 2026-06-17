import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import AmazonWarehousingAndDistribution
from sp_api.util import throttle_retry, load_all_pages
from sp_api.base.exceptions import SellingApiRequestThrottledException, SellingApiServerException
from requests.exceptions import ReadTimeout, ConnectionError
from utility import to_list
import pandas as pd
import logging
import logger_setup
import bigquery_utils


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

AWD_VERSION = '2024-05-09'
TENANTS = bigquery_utils.get_tenants()
AWD_VERSION = '2024-05-09'

PROJECT_ID = "modern-sublime-383117"
DEST_DATASET = "awd"
DEST_TABLE = "inventory"


throttle_retry()
@load_all_pages()
def list_all_inventory(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    try:
        inventory = AmazonWarehousingAndDistribution(
                                account=f'{account}-{marketplace}', 
                                marketplace=Marketplaces[marketplace], 
                                version=AWD_VERSION).list_inventory(**kwargs)
    except (SellingApiRequestThrottledException, SellingApiServerException, ReadTimeout, ConnectionError) as e:
        logger.error(f"Error updating awd inventory for {account}: {e}")

    return inventory


def get_all_inventory(account='Bare Barrel', marketplaces=['US'], **kwargs): # AWD is only available in the US for now
    """
    Combines AWD inventory payload
    Args:
        sku (string): Filter by seller or merchant SKU for the item.
        sortOrder (string): Sort the response in ASCENDING or DESCENDING order.
        details (string): Set to SHOW to return summaries with additional inventory details. Defaults to HIDE, which returns only inventory summary totals.
        nextToken (string): Token to retrieve the next set of paginated results.
        maxResults (integer): Maximum number of results to return.

    Returns:
        df
    """
    inventory_data = pd.DataFrame()
    date_today = dt.date.today()

    for marketplace in to_list(marketplaces):
        logger.info(f"Retrieving AWD inventory from {account}-{marketplace}")
        response = list_all_inventory(account, marketplace, **kwargs)

        for page in response:
            payload = page.payload.get('inventory')
            data = pd.json_normalize(payload)
            data.insert(0, 'date', date_today)
            data.insert(1, 'tenant_id', TENANTS[account])
            data.columns = data.columns.str.replace('.', '_', regex=False)
            inventory_data = pd.concat([inventory_data, data], ignore_index=True)

    return inventory_data


def update_data():
    """
    Loads awd inventory for the day to BigQuery.
    """
    # Guard clause
    if bigquery_utils.already_loaded_today(PROJECT_ID, DEST_DATASET, DEST_TABLE, "loaded_at"):
        logger.info("Data for today already exists. Skipping execution.")
        return

    inventory_data = pd.DataFrame()

    for account in TENANTS.keys():
        data = get_all_inventory(account, details='SHOW', maxResults=200)
        inventory_data = pd.concat([inventory_data, data], ignore_index=True)

    inventory_data["loaded_at"] = pd.Timestamp.now(tz="UTC")

    # inventory_data.to_csv("output.csv", index=False, encoding="utf-8")

    # Load data to BigQuery
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(inventory_data, table_id, PROJECT_ID, "append")


if __name__ == '__main__':
    update_data()