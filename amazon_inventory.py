from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from sp_api.api import Inventories
from sp_api.util import throttle_retry, load_all_pages
import postgresql
import pandas as pd
import io
import datetime as dt
import pytz


table_name = 'inventory.fba'

def get_data(marketplace='US'):
    """
    Requests inventory data and returns a pandas dataframe.
    It could could only request a snapshot of current inventory.
    """
    print(f"Getting inventory for {marketplace} marketplace.")
    inventory = Inventories(marketplace=Marketplaces[marketplace]).get_inventory_summary_marketplace(details=True)
    data = pd.json_normalize(inventory.payload['inventorySummaries'], sep='_')

    # removing top level of column name to meet postgresql col char limit (59)
    data.columns = data.columns.str.replace(f'{"inventoryDetails"}_', '', regex=False)

    # manually adding marketplace and date
    data['marketplace'] = marketplace
    data['date'] = dt.datetime.now(pytz.timezone('UTC')).date()
    return data


def update_data(marketplaces=['US', 'CA']):
    for marketplace in list(marketplaces):
        data = get_data(marketplace)
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(marketplace='US', drop_table_if_exists=False):
    data = get_data(marketplace)
    with postgresql.setup_cursor() as cur:        
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Creating table {table_name}")
        postgresql.create_table(cur, file_path=data, file_extension='pandas', table_name=table_name,
                                keys="PRIMARY KEY (date, marketplace, asin)")

        print("\tAdding triggers...")
        postgresql.update_updated_at_trigger(cur, table_name)

        print("\tUpserting data")
        postgresql.upsert_bulk(table_name, data, file_extension='pandas',
                                keys = "PRIMARY KEY (date, marketplace, asin)")

    return data


if __name__ == '__main__':
    # create_table(marketplace='US', drop_table_if_exists=True)
    update_data()