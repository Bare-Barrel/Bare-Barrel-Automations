import datetime as dt
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.base import Marketplaces
from utility import to_list, reposition_columns
from amazon_reports import request_report, get_report, download_report
import time
import pandas as pd
import postgresql
import requests
import gzip
import json
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

base_table_name = 'business_reports.detail_page_sales_and_traffic'
tenants = postgresql.get_tenants()


def download_combine_reports(start_date, end_date, account, marketplace,  asin_granularity='PARENT', date_granularity='DAY'):
    """
    Requests a `Detail Page Sales and Traffic` report found in business reports in seller central.
    The report provides two granularity reports salesAndTrafficByDate and [salesAndTrafficByAsin, salesAndTrafficByParent, salesAndTrafficByChild].
    Therefore, it needed to be downloaded one day at a time to get the daily metrics at the asin granularity level.

    Args:
        start_date, end_date (str | dt.date): YYYY-MM-DD
        marketplace (str): 'US', 'CA', 'UK'
        asin_granularity (str): PARENT, CHILD, ASIN
        date_granularity (str): DAY, WEEK, MONTH

    Returns:
        combined_data (pd.DataFrame)
    """
    # Request reports
    tries = 0
    report_ids = {}
    current_date = start_date

    while current_date <= end_date:
        try:
            report_id = request_report(ReportType.GET_SALES_AND_TRAFFIC_REPORT, 
                                        account,
                                        marketplace,
                                        start_date=current_date, 
                                        end_date=current_date,
                                        asinGranularity=asin_granularity)
            report_ids[current_date] = report_id
            current_date += dt.timedelta(days=1)
            time.sleep(10)

        except Exception as error:
            if tries == 20:
                logger.error(error)
                raise error
            logger.warning(f"Error: {error}")
            time.sleep(60)
            tries += 1

    # Downloads & combines daily reports
    combined_data = pd.DataFrame()
    for date in report_ids:
        # Downloads data
        report_id = report_ids[date]
        document_id = get_report(report_id, account, marketplace)
        compressed_data = download_report(document_id, account, marketplace)

        # Decompresses data
        decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
        data = json.loads(decompressed_data)['salesAndTrafficByAsin'] # list of dicts

        # Combines data
        data = pd.json_normalize(data, sep='_')
        data.insert(0, 'date', date)
        data.insert(0, 'marketplace', marketplace)
        data.insert(0, 'tenant_id', tenants[account])
        combined_data = pd.concat([data, combined_data], ignore_index=True)

    return combined_data


def update_data(asin_granularity='PARENT', account='Bare Barrel', marketplaces=['US', 'CA', 'UK'], 
                start_date=(dt.datetime.utcnow() - dt.timedelta(days=7)), end_date=(dt.datetime.utcnow() - dt.timedelta(days=1))):
    for marketplace in to_list(marketplaces):
        logger.info(f"Updating data {asin_granularity} {account}-{marketplace} {start_date} - {end_date}")
        data = download_combine_reports(start_date, end_date, account, marketplace, asin_granularity=asin_granularity)
        table_name = base_table_name + f"_{asin_granularity.lower()}"
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(asin_granularity, drop_table_if_exists=False):
    # Requests worth 30 days sample of data
    start_date = (dt.datetime.utcnow() - dt.timedelta(days=5))
    end_date = (dt.datetime.utcnow() - dt.timedelta(days=1))
    data = download_combine_reports(start_date, end_date, 'Bare Barrel', 'US', asin_granularity=asin_granularity)

    table_name = base_table_name + f"_{asin_granularity.lower()}"

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name)

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    update_data('PARENT')
    update_data('CHILD')