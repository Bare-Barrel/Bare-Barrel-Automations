import datetime as dt
from sp_api.base import Marketplaces
from sp_api.api import Orders
from sp_api.util import throttle_retry, load_all_pages
from sp_api.base.exceptions import SellingApiRequestThrottledException, SellingApiServerException
from requests.exceptions import ReadTimeout, ConnectionError
from utility import to_list
import postgresql
import time
import pandas as pd
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

orders_table = 'orders.amazon_orders'
order_items_table = 'orders.amazon_order_items'

@throttle_retry()
@load_all_pages()
def load_all_orders(marketplace='US', **kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    return Orders(account=marketplace, marketplace=Marketplaces[marketplace]).get_orders(**kwargs)


def get_orders_items(order_ids=[], marketplace='US'):
    """
    gets detail information of multiple orders and concatenate it to a dataframe
    """
    df = pd.DataFrame()

    for order_id in to_list(order_ids):

        while True:

            try:
                logger.info(f"Getting Order ID: {order_id}")
                order_items = Orders(account=marketplace, marketplace=Marketplaces[marketplace]).get_order_items(order_id)
                data = order_items.payload.get('OrderItems')
                data = pd.json_normalize(data, sep='_')
                data['amazon_order_id'] = order_id
                data['marketplace'] = marketplace
                df = pd.concat([df, data], ignore_index=True)
                time.sleep(1)
                break

            except (SellingApiRequestThrottledException, SellingApiServerException, ReadTimeout, ConnectionError) as error:
                logger.error(error)
                time.sleep(5)

    return df


def update_data(marketplaces=['US', 'CA', 'UK'], **kwargs):
    """
    Updates orders data based from the last updated date
    """
    for marketplace in to_list(marketplaces):
        logger.info(f"Getting Orders from {marketplace}. . .")

        # gets latest orders
        with postgresql.setup_cursor() as cur:
            cur.execute(f"SELECT MAX(last_update_date)::TIMESTAMP FROM {orders_table} WHERE marketplace = '{marketplace}';")
            last_update_date = cur.fetchone()['max']
            if not last_update_date:
                last_update_date = dt.date(2022,1,1)

        orders_data = pd.DataFrame()
        total_orders = 0
  
        for page in load_all_orders(marketplace, **kwargs):    # datetime.utcnow() - timedelta(days=290)).isoformat()

                orders_payload = page.payload.get('Orders')
                data = pd.json_normalize(orders_payload, sep='_')
                data['marketplace'] = marketplace
                orders_data = pd.concat([orders_data, data], ignore_index=True)
                total_orders += len(orders_payload)
                logger.info(f"\t{total_orders} orders processed")
                time.sleep(10)

        if orders_data.empty:
            logger.info("No new updated orders")
            continue

        orders_data.columns = [postgresql.sql_standardize(col) for col in orders_data.columns]

        # gets order items data
        order_ids = list(orders_data['amazon_order_id'].unique())
        order_items_data = get_orders_items(order_ids, marketplace)
    
        # upserts orders data
        postgresql.upsert_bulk(orders_table, orders_data, file_extension='pandas')
        postgresql.upsert_bulk(order_items_table, order_items_data, file_extension='pandas')


def create_orders_table(drop_table_if_exists=False):
    # creates table initially
    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {orders_table};")

        postgresql.create_table(cur, data, table_name=orders_table, file_extension='json_normalize', keys='PRIMARY KEY (amazon_order_id)')
        postgresql.update_updated_at_trigger(cur, orders_table)
        postgresql.upsert_bulk(orders_table, data, file_extension='json_normalize')


def create_order_items_table(drop_table_if_exists=False):
    with postgresql.setup_cursor() as cur:
        # retrieves first 50 orders for table creation
        cur.execute(f"SELECT amazon_order_id FROM {orders_table} LIMIT 50;")
        order_ids = [order['amazon_order_id'] for order in cur.fetchall()]
        data = get_orders_items(order_ids)

        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {order_items_table};")

        postgresql.create_table(cur, data, table_name=order_items_table, file_extension='pandas', keys=f'''PRIMARY KEY (order_item_id),
                                                                                                            CONSTRAINT {order_items_table.split('.')[1]}_fk 
                                                                                                            FOREIGN KEY (amazon_order_id) 
                                                                                                            REFERENCES {orders_table} (amazon_order_id)''')
        postgresql.update_updated_at_trigger(cur, order_items_table)
        postgresql.upsert_bulk(order_items_table, data, file_extension='pandas')


def update_missing_order_items(marketplaces=['US', 'CA', 'UK']):
    for marketplace in to_list(marketplaces):
        while True:
            with postgresql.setup_cursor() as cur:
                # retrieves first 50 orders for table creation
                cur.execute(f"""SELECT amazon_order_id FROM {orders_table}
                                WHERE amazon_order_id NOT IN (SELECT amazon_order_id FROM {order_items_table})
                                AND marketplace = '{marketplace}'
                                ORDER BY purchase_date DESC LIMIT 100;""")

                order_ids = [order['amazon_order_id'] for order in cur.fetchall()]

            if not order_ids:
                logger.info(f"No Missing Amazon Order IDs in orders.amazon_order_items ({marketplace})")
                break

            data = get_orders_items(order_ids)
            
            postgresql.upsert_bulk(order_items_table, data, file_extension='pandas')


if __name__ == '__main__':
    update_data(CreatedAfter=dt.date(2023,9,20).isoformat())
    update_missing_order_items()