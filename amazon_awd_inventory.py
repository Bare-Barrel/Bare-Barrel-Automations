import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import AmazonWarehousingAndDistribution
from sp_api.util import throttle_retry, load_all_pages
from sp_api.base.exceptions import SellingApiRequestThrottledException, SellingApiServerException
from requests.exceptions import ReadTimeout, ConnectionError
from utility import to_list
import pandas as pd
import postgresql
import time
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

awd_version = '2024-05-09'

throttle_retry()
@load_all_pages()
def list_all_inbound_shipments_summary(marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """

    inbound_shipments_summary = AmazonWarehousingAndDistribution(
                                    account=marketplace, 
                                    marketplace=Marketplaces[marketplace], 
                                    version=awd_version).list_inbound_shipments(**kwargs)
    return inbound_shipments_summary


throttle_retry()
@load_all_pages()
def list_all_inventory(marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    inventory = AmazonWarehousingAndDistribution(
                            account=marketplace, 
                            marketplace=Marketplaces[marketplace], 
                            version=awd_version).list_inventory(**kwargs)
    return inventory


def get_all_inbound_shipments_summary(marketplaces=['US', 'CA', 'UK'], **kwargs):
    """
    Combines inbound AWD shipments summary payload

    Args:
        sortBy (string): Field to sort results by. Required if sortOrder is provided.
        sortOrder (string): Sort the response in ASCENDING or DESCENDING order.
        shipmentStatus (string): Filter by inbound shipment status.
        updatedAfter (string): List the inbound shipments that were updated after a certain time (inclusive). The date must be in <a href=’https://developer-docs.amazon.com/sp-api/docs/iso-8601’>ISO 8601</a> format.
        updatedBefore (string): List the inbound shipments that were updated before a certain time (inclusive). The date must be in <a href=’https://developer-docs.amazon.com/sp-api/docs/iso-8601’>ISO 8601</a> format.
        maxResults (integer): Maximum number of results to return.
        nextToken (string): Token to retrieve the next set of paginated results.

    Returns:
        df
    """
    shipments_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        logger.info(f'Retrieving AWD inbound shipments from {marketplace}')
        response = list_all_inbound_shipments_summary(marketplace, **kwargs)

        for page in response:
            payload = page.payload.get('shipments')
            data = pd.json_normalize(payload)
            data.insert(0, 'marketplace', marketplace)
            shipments_data = pd.concat([shipments_data, data], ignore_index=True)

    # Renaming createdAt & updatedAt to match Sellercentral report 
    # and to avoid conflict with update triggers
    shipments_data.rename(
        columns={
            'createdAt': 'shipment_create_date_utc',
            'updatedAt': 'shipment_last_update_date_utc'
    }, inplace=True)

    return shipments_data


def get_all_inventory(marketplaces=['US', 'CA', 'UK'], **kwargs):
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

    for marketplace in to_list(marketplaces):
        logger.info(f"Retrieving AWD inventory from {marketplace}")
        response = list_all_inventory(marketplace, **kwargs)

        for page in response:
            payload = page.payload.get('inventory')
            data = pd.json_normalize(payload)
            data.insert(0, 'date', dt.date.today())
            data.insert(0, 'marketplace', marketplace)
            inventory_data = pd.concat([inventory_data, data], ignore_index=True)

    return inventory_data


def get_all_inbound_shipments(marketplaces=['US', 'CA', 'UK'], **kwargs):
    """
    Retrieves and combines detailed inbound shipments data
    Args: 
        skuQuantities (string): Set SHOW to include the shipment SKU quantity details
                                Defaults to HIDE which returns shipment without SKU quantities.
    
    Returns:
        df
    """
    inbound_shipments_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        logger.info(f"Querying all shipment ids in {marketplace}")
        query = f"""
                SELECT DISTINCT shipment_id FROM inventory.awd_inbound_shipments_summary
                WHERE marketplace = '{marketplace}';
                """
        shipment_ids = postgresql.sql_to_dataframe(query)

        for row in shipment_ids.values:
            shipment_id = row[0]
            logger.info(f"\tGetting shipment id: {shipment_id}") 
            response = AmazonWarehousingAndDistribution(
                                            account=marketplace, 
                                            marketplace=Marketplaces[marketplace], 
                                            version=awd_version).get_inbound_shipment(shipmentId=shipment_id, **kwargs)
            payload = response.payload
            data = pd.json_normalize(payload)
            data.insert(0, 'date', dt.date.today())
            data.insert(0, 'marketplace', marketplace)
            inbound_shipments_data = pd.concat([inbound_shipments_data, data], ignore_index=True)

            time.sleep(2)

    # Renaming createdAt & updatedAt to match Sellercentral report 
    # and to avoid conflict with update triggers
    inbound_shipments_data.rename(
        columns={
            'createdAt': 'shipment_create_date_utc',
            'updatedAt': 'shipment_last_update_date_utc'
    }, inplace=True)

    return inbound_shipments_data


def update_data(table_name, marketplaces=['US', 'CA', 'UK']):
    """
    Upserts awd inventory for the day.
    """
    table_names = {
        'inventory.awd_inbound_shipments_summary': lambda: get_all_inbound_shipments_summary(marketplaces),
        'inventory.awd_inventory': lambda: get_all_inventory(marketplaces, details='SHOW', maxResults=200),
        'inventory.awd_inbound_shipments': lambda: get_all_inbound_shipments(marketplaces, skuQuantities='SHOW')
    }
    data = table_names[table_name]()

    logger.info(f"Upserting table {table_name}")
    postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(table_name, drop_table_if_exists=False):
    table_names = {
        'inventory.awd_inbound_shipments_summary': lambda: get_all_inbound_shipments_summary(['US', 'CA']),
        'inventory.awd_inventory': lambda: get_all_inventory(['US', 'CA'], details='SHOW', maxResults=200),
        'inventory.awd_inbound_shipments': lambda: get_all_inbound_shipments(['US', 'CA'], skuQuantities='SHOW')
    }
    data = table_names[table_name]()

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name)

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    table_names = ['inventory.awd_inbound_shipments_summary', 
                   'inventory.awd_inventory',
                   'inventory.awd_inbound_shipments']

    for table_name in table_names:
        update_data(table_name, ['US', 'CA'])
