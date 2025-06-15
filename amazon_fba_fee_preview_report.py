import datetime as dt
from sp_api.base import Marketplaces
from amazon_reports import request_report, get_report, download_report
from sp_api.base.reportTypes import ReportType
from sp_api.base.exceptions import SellingApiRequestThrottledException
import time
import pandas as pd
import numpy as np
import postgresql
import logging
import logger_setup
from utility import to_list
from io import BytesIO

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_name = 'business_reports.fba_fee_preview'
tenants = postgresql.get_tenants()


def request_reports(start_date, end_date, account='Bare Barrel', marketplace='US'):
    """
    Contains the estimated Amazon Selling and Fulfillment Fees for the seller's FBA inventory 
    with active offers. The content is updated at least once every 72 hours. 
    To successfully generate a report, specify the StartDate parameter for a minimum 72 hours 
    prior to NOW and EndDate to NOW.
    """
    # Request reports
    tries = 0
    report_ids = []

    while start_date <= end_date:
        try:
            report_id = request_report(ReportType.GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA, 
                                        account,
                                        marketplace,
                                        start_date=start_date, 
                                        end_date=end_date)
            report_ids.append(report_id)
            start_date += dt.timedelta(days=7)
            tries += 1
            time.sleep(1 * 46 if tries > 0 else 1)

        except SellingApiRequestThrottledException as error:
            logger.warning(error)
            time.sleep(60)

    return report_ids


def download_combine_reports(report_ids, account='Bare Barrel', marketplace='US'):
    # Downloads & combines daily reports
    combined_data = pd.DataFrame()
    for report_id in report_ids:
        # Downloads data
        document_id = get_report(report_id, account, marketplace)
        raw_bytes = download_report(document_id, account, marketplace)

        if not raw_bytes:
            logger.warning(f"\tReport {report_id} is empty or failed to download.")
            continue

        # Convert tab-delimited bytes string to Pandas DataFrame
        print(raw_bytes)
        data = pd.read_csv(BytesIO(raw_bytes), sep="\t", dtype=str, encoding='latin1')

        if not data.empty:
            # Combines data
            data.insert(0, 'marketplace', marketplace)
            data.insert(0, 'tenant_id', tenants[account])
            data.insert(0, 'date', dt.date.today())
            combined_data = pd.concat([data, combined_data], ignore_index=True)

    combined_data.replace("--", np.nan, inplace=True)                                        

    return combined_data


def update_data(start_date, end_date, account='Bare Barrel', marketplaces=['US', 'CA', 'UK']):
    logger.info(f"Updating FBA Fee Preview Report {account}-{marketplaces} {start_date} - {end_date}")

    # Requests report
    marketplace_report_ids = {}

    for marketplace in to_list(marketplaces):
        marketplace_report_ids[marketplace] = []

        report_ids = request_reports(start_date, end_date, account=account, marketplace=marketplace)
        marketplace_report_ids[marketplace] += report_ids

    # Downloads, cleans & combines report
    data = pd.DataFrame()

    for marketplace in to_list(marketplaces):
        report_ids = marketplace_report_ids[marketplace]
        df = download_combine_reports(report_ids, account=account, marketplace=marketplace)
        data = pd.concat([data, df], ignore_index=True)

    with postgresql.setup_cursor() as cur:
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')

    return data


def create_table(drop_table_if_exists=False):
    start_date, end_date = dt.date.today(), dt.date.today()
    data = download_combine_reports(start_date, end_date, 'Bare Barrel', 'US')

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name)

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    date_yesterday = dt.date.today() - dt.timedelta(days=3)
    start_date = dt.date.today() - dt.timedelta(days=3)
    end_date = dt.date.today() - dt.timedelta(days=1)

    for account in tenants.keys():
        update_data(start_date, end_date, account=account)