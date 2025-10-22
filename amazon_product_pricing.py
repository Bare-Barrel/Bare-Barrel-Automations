import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import Products
from sp_api.base.exceptions import SellingApiRequestThrottledException
from utility import to_list, reposition_columns
import postgresql
import time
import pandas as pd
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

product_pricing_table = 'product_pricing.competitive_pricing'
tenants = postgresql.get_tenants()


def get_competitive_pricing(asin_list, customer_type='Consumer', item_condition='New', account='Bare Barrel', marketplace='US'):
    """
    Competitive pricing information for a seller's offer listings based on seller SKU or ASIN.
    Iterates for every 20 ASINs, combines payload and returns combined dataframe.

    Args:
        asin_list (list): Max 20 ASINs per call.
        customer_type (str): Consumer - Non-prime price
                             Business - Prime price
        item_condition (str): New, Used, Collectible, Refurbished, Club

    Returns:
        combined_data (pd.DataFrame)
    """
    print(marketplace)
    logger.info(f"Getting {item_condition} Competitive {customer_type} Pricing of {len(asin_list)} ASINs {account}-{marketplace}")
    combined_data = pd.DataFrame()
    # Makes call for every 20 asins
    max_asins = 20
    total_processed = 0

    for i in range(0, len(asin_list), max_asins):
        asins = asin_list[i:i+max_asins]
        total_processed += len(asins)

        while True:            

            try:
                logger.info('\t' + f"Processing {total_processed} / {len(asin_list)}. . .")
                response = Products(account=f'{account}-{marketplace}', marketplace=Marketplaces[marketplace]).get_competitive_pricing_for_asins(asin_list=asins, item_condition='New', customer_type=customer_type)
                data = pd.json_normalize(response.payload, sep='_')
                data['date'] = dt.date.today()
                data['customer_type'] = customer_type
                data['marketplace'] = marketplace
                data['tenant_id'] = tenants[account]
                combined_data = pd.concat([data, combined_data], ignore_index=True)
                time.sleep(2)
                break

            except SellingApiRequestThrottledException as error:
                logger.warning(f'\t{error}')
                time.sleep(5)

    combined_data = reposition_columns(combined_data, {'date': 1, 'marketplace': 2, 'customer_type': 3})
    return combined_data


def get_listings_prime_prices(customer_type='Consumer', account='Bare Barrel', marketplace='US'):
    """
    Gets active listings products prime prices
    """
    # Gets active listing asins
    with postgresql.setup_cursor() as cur:
        cur.execute(f"""
                    SELECT asin FROM listings_items.summaries
                    WHERE status @> ARRAY['DISCOVERABLE']
                        AND marketplace = '{marketplace}'
                        AND tenant_id = {tenants[account]}
                        AND date = (select max(date) from listings_items.summaries
                                    where marketplace = '{marketplace}')
                                        and tenant_id = {tenants[account]};
                    """)
        active_asins = [row['asin'] for row in cur.fetchall()]

    data = get_competitive_pricing(active_asins, customer_type=customer_type, 
                                    account=account, marketplace=marketplace)
    data['tenant_id'] = tenants[account]
    return data


def update_data(customer_types=['Consumer', 'Business'], account='Bare Barrel', marketplaces=['US', 'CA', 'UK']):
    logger.info(f"Updating Competitive Pricing Table {account}-{marketplaces}")
    combined_data = pd.DataFrame()

    for customer_type in to_list(customer_types):

        for marketplace in to_list(marketplaces):
            data = get_listings_prime_prices(customer_type, account, marketplace)
            combined_data = pd.concat([combined_data, data], ignore_index=True)

    postgresql.upsert_bulk(product_pricing_table, combined_data, 'pandas')


def create_table(table_name=product_pricing_table, drop_table_if_exists=False):
    data = get_listings_prime_prices()

    logger.info(f"Creating Table: {table_name}")

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        primary_keys = 'asin, date, marketplace, customer_type'

        postgresql.create_table(cur, data, table_name=table_name, file_extension='pandas', 
                                                keys=f'PRIMARY KEY ({primary_keys})')
        postgresql.update_updated_at_trigger(cur, table_name)
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')
    

if __name__ == '__main__':
    for account in tenants.keys():
        update_data(account=account)