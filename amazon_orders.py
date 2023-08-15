from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from sp_api.api import Orders
from sp_api.util import throttle_retry, load_all_pages
import postgresql


table_name = 'orders.amazon'

@throttle_retry()
@load_all_pages()
def load_all_orders(**kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    return Orders().get_orders(**kwargs)


def update_data():
    """
    Updates orders data based from the last updated date
    """
    with postgresql.setup_cursor() as cur:
        cur.execute(f"SELECT MAX(last_update_date)::TIMESTAMP FROM {table_name};")
        last_update_date = cur.fetchone()['max']

    orders = 0
    for page in load_all_orders(LastUpdatedAfter=last_update_date.isoformat()):    # datetime.utcnow() - timedelta(days=290)).isoformat()
        data = page.payload.get('Orders')
        orders += len(data)
        postgresql.upsert_bulk(table_name, data, file_extension='json_normalize')
        print(orders)


def create_table():
    # creates table initially
    with postgresql.setup_cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders.amazon;")
        postgresql.create_table(cur, data, table_name=table_name, file_extension='json_normalize', keys='PRIMARY KEY (amazon_order_id)')
        postgresql.update_updated_at_trigger(cur, table_name)
        postgresql.upsert_bulk(table_name, data, file_extension='json_normalize')


if __name__ == '__main__':
    update_data()