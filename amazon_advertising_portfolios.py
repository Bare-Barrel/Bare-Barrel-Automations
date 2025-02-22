import logging
from ad_api.api import Portfolios
from ad_api.base import AdvertisingApiException
from ad_api.base import Marketplaces
from utility import to_list
import pandas as pd
import postgresql
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

table_name = 'amazon_advertising_portfolios'
tenants = postgresql.get_tenants()


def list_portfolios(account='Bare Barrel', marketplace='US', **kwargs):
    logging.info(f"Getting portfolios {account}-{marketplace}")
    try:
        result = Portfolios(account=f'{account}-{marketplace}', 
                            marketplace=Marketplaces[marketplace]).list_portfolios_extended(**kwargs)

        if result.payload:
            payload = result.payload
            for portfolio in payload:
                logging.info(portfolio)
        else:
            logging.info(result)

    except AdvertisingApiException as error:
        logging.error(error)

    return payload


def update_data(account='Bare Barrel', marketplaces=['US', 'CA', 'UK']):
    combined_data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        response = list_portfolios(account=account, marketplace=marketplace)
        data = pd.json_normalize(response)
        data['marketplace'] = marketplace
        data['tenant_id'] = tenants[account]
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    logging.info("Upserting data")
    postgresql.upsert_bulk(table_name, combined_data, file_extension='pandas')


def create_table(drop_table_if_exists=False):
    response = list_portfolios(marketplace='US')
    data = pd.json_normalize(response)
    data['marketplace'] = 'US'
    logger.info(data)

    with postgresql.setup_cursor() as cur:        
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        logger.info(f"Creating table {table_name}")
        postgresql.create_table(cur, file_path=data, file_extension='pandas', table_name=table_name,
                                    keys='PRIMARY KEY (portfolio_id)')

        logger.info("\tAdding triggers...")
        postgresql.update_updated_at_trigger(cur, table_name)

        logger.info("\tUpserting data")
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    update_data()