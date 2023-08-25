import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import ListingsItems
from sp_api.base.exceptions import SellingApiNotFoundException
import postgresql
import time
import pandas as pd
import json
from utility import to_list, flatten_json_list_values, reposition_columns
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)
    seller_id = config['amazon_seller_id']

listings_items_schema = 'listings_items'
table_names = {'summaries': 'summaries', 'attributes': 'attributes', 'issues': 'issues', 'offers': 'offers', 
                'fulfillment_availability': 'fulfillmentAvailability', 'procurement': 'procurement'}


def get_all_listings_items(included_data=['summaries'], marketplaces=['US', 'CA']):
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
    listings_data = pd.DataFrame()

    for marketplace in to_to_list(marketplaces):
        logger.info(f"Getting listing items {str(included_data)} in {marketplace}")

        # Retrieves all skus per marketplace
        with postgresql.setup_cursor() as cur:
            cur.execute(f"""SELECT DISTINCT seller_sku 
                            FROM inventory.fba WHERE marketplace = '{marketplace}';""")
            skus = [sku['seller_sku'] for sku in cur.fetchall()]
        
        for sku in skus:
            logger.info(f"\tProcessing SKU: {sku}")

            while True:
                try:
                    response = ListingsItems(marketplace=Marketplaces[marketplace]).get_listings_item(seller_id, sku, 
                                                                            includedData=','.join(to_list(included_data)))
                    flatten_payload = flatten_json_list_values(response.payload)
                    data = pd.json_normalize(flatten_payload, sep='_')
                    data['marketplace'] = marketplace
                    data['date'] = dt.datetime.utcnow().date()
                    listings_data = pd.concat([data, listings_data], ignore_index=True)
                    time.sleep(0.25)
                    break
                except Exception as error:
                    logger.error(error)
                    if type(error) == SellingApiNotFoundException:
                        break
                    time.sleep(30)

    return listings_data


def update_data(tables=table_names.keys(), marketplaces=['US', 'CA']):
    # Gets all included data in one response
    included_data = [table_names[table_name] for table_name in tables]
    data = get_all_listings_items(included_data, marketplaces)
    data.columns = [postgresql.sql_standardize(col) for col in data.columns]
    
    # Splits data by their columns in the database
    for table_name in table_names:
        with postgresql.setup_cursor() as cur:
            cur.execute(f"""SELECT column_name FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            AND table_schema = '{listings_items_schema}'""")

            columns = [col['column_name'] for col in cur.fetchall() if col['column_name'] not in ('created_at', 'updated_at')]

            # Upserts
            table_name = listings_items_schema + '.' + table_name
            postgresql.upsert_bulk(table_name, data[columns], 'pandas')


def create_table(table_name, drop_table_if_exists=False):
    logger.info(f"Creating Table: {table_name}")
    included_data = table_names[table_name]
    data = get_all_listings_items(included_data)
    data = reposition_columns(data, {'date': 0, 'marketplace': 1})

    table_name = listings_items_schema + '.' + table_name

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, table_name=table_name, file_extension='pandas', 
                                                keys='PRIMARY KEY (sku, date, marketplace)')
        postgresql.update_updated_at_trigger(cur, table_name)
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    update_data()