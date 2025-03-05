from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from sp_api.api import Inventories
from sp_api.util import throttle_retry, load_all_pages
from utility import to_list
import postgresql
import pandas as pd
import io
import datetime as dt
import pytz
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_name = 'inventory.fba'
tenants = postgresql.get_tenants()


@throttle_retry()
@load_all_pages(next_token_param='nextToken')
def load_all_inventories(account='Bare Barrel', marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    return Inventories(account=f'{account}-{marketplace}', 
                        marketplace=Marketplaces[marketplace]).get_inventory_summary_marketplace(**kwargs)


def get_data(account='Bare Barrel', marketplace='US', **kwargs):
    """
    Requests inventory data and returns a pandas dataframe.
    It could could only request a snapshot of current inventory.
    """
    logger.info(f"Getting inventory for {account}-{marketplace}")

    combined_data = pd.DataFrame()

    for inventory in load_all_inventories(account, marketplace, **kwargs):
        print(inventory.payload)
        data = pd.json_normalize(inventory.payload['inventorySummaries'], sep='_')
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    # removing top level of column name to meet postgresql col char limit (59)
    combined_data.columns = data.columns.str.replace(f'{"inventoryDetails"}_', '', regex=False)

    # manually adding marketplace and date
    combined_data['marketplace'] = marketplace
    combined_data['date'] = dt.datetime.now(pytz.timezone('UTC')).date()
    combined_data['tenant_id'] = tenants[account]

    return combined_data


def update_data(account='Bare Barrel', marketplaces=['US', 'CA', 'UK']):
    for marketplace in to_list(marketplaces):
        data = get_data(account, marketplace, details=True)
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(account='Bare Barrel', marketplace='US', drop_table_if_exists=False):
    data = get_data(account, marketplace, details=True)

    with postgresql.setup_cursor() as cur:

        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        logger.info(f"Creating table {table_name}")

        postgresql.create_table(cur, file_path=data, file_extension='pandas', table_name=table_name,
                                keys="PRIMARY KEY (date, marketplace, asin)")

        logger.info("\tAdding triggers...")
        postgresql.update_updated_at_trigger(cur, table_name)

        logger.info("\tUpserting data")
        postgresql.upsert_bulk(table_name, data, file_extension='pandas',
                                keys = "PRIMARY KEY (date, marketplace, asin)")

    return data


if __name__ == '__main__':
    for account in tenants.keys():
        update_data(account)