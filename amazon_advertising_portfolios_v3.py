import logging
from ad_api.api import PortfoliosV3
from ad_api.base import AdvertisingApiException
from ad_api.base import Marketplaces
from utility import to_list
import pandas as pd
import logger_setup
import bigquery_utils


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

TENANTS = bigquery_utils.get_tenants()
MARKETPLACES = ['US', 'CA', 'UK']

PROJECT_ID = "modern-sublime-383117"
DEST_DATASET = "amazon_ads_api"
DEST_TABLE = "amazon_advertising_portfolios_v3"


def list_portfolios(account, marketplace):
    logging.info(f"Getting portfolios for {account}-{marketplace}...")

    try:
        result = PortfoliosV3(account=f'{account}-{marketplace}', 
                            marketplace=Marketplaces[marketplace]).list_portfolios(body={})

        if result.payload:
            payload = result.payload
            logging.info(f"Done getting portfolios for {account}-{marketplace}.")

        else:
            logging.info(f"Failed to get portfolios data for {account}-{marketplace}.")
            logging.info(result)
            payload = {}

    except AdvertisingApiException as error:
        logging.error(error)

    return payload


def update_data():
    combined_data = pd.DataFrame()
    for account in TENANTS.keys():
        for marketplace in to_list(MARKETPLACES):
            payload = list_portfolios(account, marketplace)
            portfolios = payload.get("portfolios")
            
            if portfolios:
                df = pd.json_normalize(portfolios, sep='_')
                df['marketplace'] = marketplace
                df['tenant_id'] = TENANTS[account]
                combined_data = pd.concat([combined_data, df], ignore_index=True)

    combined_data["recorded_at"] = pd.Timestamp.now(tz="UTC")

    # combined_data.to_csv("output.csv", index=False, encoding="utf-8")

    # Load data to BigQuery
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(combined_data, table_id, PROJECT_ID, "append")


if __name__ == '__main__':
    update_data()