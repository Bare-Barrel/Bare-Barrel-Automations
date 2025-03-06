import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import ListingsItems
from sp_api.base.exceptions import SellingApiNotFoundException
import postgresql
import time
import pandas as pd
import json
from utility import to_list, payload_to_dataframe, reposition_columns
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)
    # seller_id = config['amazon_seller_id']

listings_items_schema = 'listings_items'
table_names = {'summaries': 'summaries', 'attributes': 'attributes', 'issues': 'issues', 'offers': 'offers', 
                'fulfillment_availability': 'fulfillmentAvailability'} #, 'procurement': 'procurement'} empty

tenants = postgresql.get_tenants()


def get_all_listings_items(included_data=['summaries', 'attributes', 'issues', 'offers', 'fulfillmentAvailability', 'procurement'], 
                            account='Bare Barrel', marketplaces=['US', 'CA', 'UK']):
    """
    Gets multiple item listings
    Args
        marketplaces (str|list)
        included_data (str|list): A comma-delimited list of data sets to included in the response.
                                [   summaries - Summary details of the listing item.
                                    attributes - JSON object containing structured listing item attribute data keyed by attribute name.
                                    issues - Issues associated with the listing item.
                                    offers - Current offers for the listing item.
                                    fulfillmentAvailability - Fulfillment availability details for the listing item.
                                    procurement - Vendor procurement details for the listing item.  ]

    Returns
        listings_data (pd.DataFrame)
    """
    seller_id = config[f'amazon_seller_id-{account}']
    # Creates empty dataframes
    listings_data = {}
    for data_set in included_data:
        listings_data[data_set] = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        logger.info(f"Getting listing items {str(included_data)} in {account}-{marketplace}")

        # Retrieves all skus per marketplace
        with postgresql.setup_cursor() as cur:
            cur.execute(f"""SELECT DISTINCT seller_sku 
                            FROM inventory.fba 
                            WHERE marketplace = '{marketplace}'
                            AND tenant_id = {tenants[account]};""")
            skus = [sku['seller_sku'] for sku in cur.fetchall()]
        
        for sku in skus:
            logger.info(f"\tProcessing SKU: {sku}")

            while True:
                try:
                    response = ListingsItems(account=f'{account}-{marketplace}', 
                                            marketplace=Marketplaces[marketplace]).get_listings_item(
                                                                                    seller_id[marketplace], 
                                                                                    sku, 
                                                                                    includedData=','.join(to_list(included_data))
                                                                                )

                    # Combies each included data set payload
                    for data_set in included_data:
                        data = payload_to_dataframe(response, get_key=data_set)
                        data['sku'] = sku
                        data['marketplace'] = marketplace
                        data['date'] = dt.datetime.now(dt.timezone.utc).date()
                        data['tenant_id'] = tenants[account]
                        data = reposition_columns(data, {'sku':0, 'date': 1, 'marketplace': 2})
                        listings_data[data_set] = pd.concat([data, listings_data[data_set]], ignore_index=True)

                    time.sleep(0.25)
                    break

                except Exception as error:
                    logger.error(error)
                    if type(error) == SellingApiNotFoundException:
                        break
                    time.sleep(30)

    return listings_data


def update_data(tables=table_names.keys(), account='Bare Barrel', marketplaces=['US', 'CA', 'UK']):
    # Gets all included data in one response
    included_data = [table_names[table_name] for table_name in tables]
    listings_data = get_all_listings_items(included_data, account, marketplaces)

    # Upserts
    for table in tables:
        data_set = table_names[table]
        data = listings_data[data_set]
        table_name = listings_items_schema + '.' + table
        postgresql.upsert_bulk(table_name, data, 'pandas')


def create_table(tables=table_names.keys(), drop_table_if_exists=False):
    # Gets listings data
    included_data = [table_names[table] for table in tables]
    listings_data = get_all_listings_items(included_data)


    for table in tables:
        table_name = listings_items_schema + '.' + table
        logger.info(f"Creating Table: {table_name}")

        data_set = table_names[table]
        data = listings_data[data_set].copy()
        data.columns = [col.replace('.', '_') for col in data.columns]

        with postgresql.setup_cursor() as cur:
            if drop_table_if_exists:
                cur.execute(f"DROP TABLE IF EXISTS {table_name};")

            primary_keys = 'sku, date, marketplace'
            if 'issues' in table_name:
                primary_keys += ', code, message'
            elif 'offers' in table_name:
                primary_keys += ', offer_type'

            postgresql.create_table(cur, data, table_name=table_name, file_extension='pandas', 
                                                    keys=f'PRIMARY KEY ({primary_keys})')
            postgresql.update_updated_at_trigger(cur, table_name)
            postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    for account in tenants.keys():
        update_data(account=account)