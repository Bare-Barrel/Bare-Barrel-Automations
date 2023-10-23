from ad_api.api.sp import CampaignsV3 as sp_campaigns
from ad_api.api.sb import CampaignsV4 as sb_campaigns
from ad_api.api.sd import Campaigns as sd_campaigns
from ad_api.base import Marketplaces
from decorators import Utils
from utility import to_list
import pandas as pd
import postgresql
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_names = {
    'sponsored_products.campaigns': sp_campaigns,
    'sponsored_brands.campaigns': sb_campaigns,
    'sponsored_display.campaigns':  sd_campaigns
}

@Utils.load_all_pages(throttle_by_seconds=1, next_token_param='nextToken')
def list_campaigns(table_name, marketplace='US', **kwargs):
    """
    Lists sponsored products, brands & display campaigns
    https://advertising.amazon.com/API/docs/en-us/guides/sponsored-products/campaigns
    https://python-amazon-ad-api.readthedocs.io/en/latest/sb/campaigns_v4.html#ad_api.api.sb.CampaignsV4.CampaignsV4.list_campaigns

    Returns 
        response (ad_api.base.api_response.ApiResponse)
    """
    logger.info(f"Getting list of {table_name} ({marketplace})")
    Campaigns = table_names[table_name]

    return Campaigns(account=marketplace, marketplace=Marketplaces[marketplace]).list_campaigns(body=kwargs)


def get_data(table_name, marketplaces=['US', 'CA', 'UK'], **kwargs):
    """
    Combines campaigns list of campaigns
    Returns
        data (pd.DataFrame)
    """
    data = pd.DataFrame()

    for marketplace in to_list(marketplaces):

        for page in list_campaigns(table_name, marketplace, **kwargs):

            if table_name == 'sponsored_display.campaigns':
                campaigns = page.payload
            else:
                campaigns = page.payload.get('campaigns')

            df = pd.json_normalize(campaigns, sep='_')

            if df.empty:
                continue
            
            df['marketplace'] = marketplace
            data = pd.concat([data, df], ignore_index=True)

    return data


def update_data(table_names=table_names.keys(), marketplaces=['US', 'CA', 'UK'], **kwargs):
    for table_name in table_names:
        data = get_data(table_name, marketplaces, **kwargs)
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(table_name, drop_table_if_exists=False):
    data = get_data(table_name)

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        logger.info(f"Creating table {table_name}")
        postgresql.create_table(cur, file_path=data, file_extension='pandas', table_name=table_name,
                                    keys='PRIMARY KEY (campaign_id)')

        logger.info("\tAdding triggers...")
        postgresql.update_updated_at_trigger(cur, table_name)

        logger.info("\tUpserting data")
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    update_data()