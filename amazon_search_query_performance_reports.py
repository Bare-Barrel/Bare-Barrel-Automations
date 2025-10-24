import datetime as dt
# from sp_api.base import Marketplaces
from amazon_reports import request_report, get_report, download_report
from sp_api.base.reportTypes import ReportType
from sp_api.base.exceptions import SellingApiRequestThrottledException
import time
import pandas as pd
import postgresql
# import requests
import gzip
import json
from utility import get_day_of_week, to_list
import logging
import logger_setup
# import os
import argparse


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_name = 'brand_analytics.search_query_performance_weekly_asin' # Brand view not yet available on sp-api!
tenants = postgresql.get_tenants()


def request_reports(asin, start_date, end_date, account='Bare Barrel', marketplace='US'):
    """
    Provides overall query performance, such as impressions, clicks, cart adds, and purchases for a given ASIN and specified date range. Data is available at different reporting periods: WEEK, MONTH, and QUARTER. Requests cannot span multiple periods. For example, a request at WEEK level could not start on 2025-01-05 and end on 2025-01-18 as this would span two weeks.

    This report accepts the following reportOptions values:

    asin - (Required) The Amazon Standard Identification Number (ASIN) for which you want data.
    reportPeriod - Specifies the reporting period for the report. Values include WEEK, MONTH, and QUARTER. Example: "reportOptions":{"reportPeriod": "WEEK"}
    Requests must include the reportPeriod in the reportsOptions. Use the dataStartTime and dataEndTime parameters to specify the date boundaries for the report. The dataStartTime and dataEndTime values must correspond to valid first and last days in the specified reportPeriod. For example, dataStartTime** must be a Sunday and dataEndTime must be a Saturday when reportPeriod=WEEK.
    """
    # Gets Sunday & Saturday
    start_date = get_day_of_week(start_date, 'Sunday')
    end_date = get_day_of_week(end_date, 'Saturday')

    # Request reports
    tries = 0
    report_ids = []

    while start_date <= end_date:
        try:
            end_of_week = get_day_of_week(start_date, 'Saturday') # Gets Saturday of the week
            report_id = request_report(ReportType.GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT, 
                                        account,
                                        marketplace,
                                        start_date=start_date, 
                                        end_date=end_of_week,
                                        reportPeriod="WEEK",
                                        asin=asin)
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
        compressed_data = download_report(document_id, account, marketplace)

        # Decompresses data
        decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
        data = json.loads(decompressed_data)['dataByAsin'] # list of dicts

        # Cleans column names
        data = pd.json_normalize(data, sep='_')
        data.columns = [
            "_".join(col.split("_")[1:]) if "_" in col else col 
            for col in data.columns
        ]

        if not data.empty:
            # Combines data
            data.insert(0, 'marketplace', marketplace)
            data.insert(0, 'tenant_id', tenants[account])
            combined_data = pd.concat([data, combined_data], ignore_index=True)

    return combined_data


def update_data(start_date, end_date, asins='All', account='Bare Barrel', marketplace='US'):
    logger.info(f"Updating SQP data {asins} {account}-{marketplace} {start_date} - {end_date}")

    def get_asins(start_date, end_date, account, marketplace):
        with postgresql.setup_cursor() as cur:
            cur.execute(f"""
                        SELECT DISTINCT asin 
                        FROM listings_items.summaries 
                        WHERE 
                            marketplace = '{marketplace}'
                            AND tenant_id = {tenants[account]}
                            AND date >= '{start_date}'::DATE - INTERVAL '6 days' 
                            AND date <= '{end_date}'::DATE
                            AND status @> ARRAY['DISCOVERABLE', 'BUYABLE']
            ;""")
            asins = [asin['asin'] for asin in cur.fetchall()]
            return asins

    # Requests report
    marketplace_report_ids = {}
    marketplace_report_ids[marketplace] = []

    if asins == 'All':
        # Gets all active ASINs
        asins = get_asins(start_date, end_date, account, marketplace)
        logger.info(f"Getting all active ASINs in {account}-{marketplace} {start_date} - {end_date}")

    for asin in to_list(asins):
        report_ids = request_reports(asin, start_date, end_date, account=account, marketplace=marketplace)
        marketplace_report_ids[marketplace] += report_ids

    # Downloads, cleans & combines report
    data = pd.DataFrame()

    report_ids = marketplace_report_ids[marketplace]
    df = download_combine_reports(report_ids, account=account, marketplace=marketplace)
    data = pd.concat([data, df], ignore_index=True)

    postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Request reports for a specific account."
    )
    parser.add_argument(
        "--account",
        type=str,
        choices=["Bare Barrel", "Rymora"],
        required=True,
        default="Bare Barrel",
        help="Name of the account to process ('Bare Barrel', 'Rymora')"
    )
    parser.add_argument(
        "--marketplace",
        type=str,
        choices=["US", "CA", "UK"],
        required=True,
        default="US",
        help="Name of the marketplace to process ('US', 'CA', 'UK')"
    )

    return parser.parse_args()


if __name__ == '__main__':
    last_week = dt.date.today() - dt.timedelta(weeks=1)
    end_date = get_day_of_week(last_week, 'Saturday')
    start_date = end_date - dt.timedelta(weeks=2)

    args = parse_args()

    update_data(start_date, end_date, asins='All', account=args.account, marketplace=args.marketplace)