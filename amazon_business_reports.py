import datetime as dt
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.base import Marketplaces
from utility import to_list
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


def request_report(start_date, end_date, marketplace='US', asin_granularity='PARENT', date_granularity='DAY'):
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
        data (pd.DataFrame): 
    """
    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
        end_date   = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # Sets start date (00:00:00) and end date (23:59:59)
    start_date = dt.datetime.combine(start_date, dt.time.min)
    end_date = dt.datetime.combine(end_date, dt.time.max)

    logger.info(f"Requesting Sales & Traffic Report ({marketplace}) {asin_granularity} {str(start_date)} - {str(end_date)}")

    response = ReportsV2(account=marketplace, marketplace=Marketplaces[marketplace]).create_report(
                            reportType = ReportType.GET_SALES_AND_TRAFFIC_REPORT,
                            reportOptions = {
                                            "dateGranularity": date_granularity,
                                            "asinGranularity": asin_granularity
                            },
                            # optionally, you can set a start and end time for your report
                            dataStartTime = start_date.isoformat(),
                            dataEndTime = end_date.isoformat()
    )

    report_id = response.payload['reportId']

    return report_id


def get_report(report_id):
    """
    Checks and waits for the report status to be downloaded.

    Returns document_id (str)
    """
    result = ReportsV2().get_report(report_id)
    payload = result.payload
    status = payload['processingStatus']
    logger.info(f"Report Processing Status: {status}")

    if payload['processingStatus'] == 'DONE':
        document_id = result.payload['reportDocumentId']
        if document_id:
            return document_id

    time.sleep(15)
    return get_report(report_id)


def download_report(document_id):
    """
    Download and decompresses zipped report to memory.

    Returns data (json)
    """
    try:
        response = ReportsV2().get_report_document(document_id)
        url = response.payload['url']
        response = requests.get(url)

        if response.status_code == 200:
            logger.info("\tFile downloaded successfully.")
            compressed_data = response.content

            # Decompresses data
            decompressed_data = gzip.decompress(compressed_data).decode('utf-8')

            # Parse salesAndTrafficByAsin
            data = json.loads(decompressed_data)['salesAndTrafficByAsin'] # list of dicts

            return data

        time.sleep(1)

    except Exception as error:
        logger.warning(f"Error {error}")
        time.sleep(30)

        # Retries
        return download_report(document_id)



def download_combine_reports(start_date, end_date, marketplace, asin_granularity):
    """
    Requests and downloads daily reports. Parsed the zipped json file. 
    Combines reports and returns pandas dataframe at asin granularity level.

    Args:
        start_date, end_date (str | dt.date): YYYY-MM-DD
        marketplace (str): 'US', 'CA', 'UK'
        asin_granularity (str): PARENT, CHILD, ASIN

    Returns:
        data (pd.DataFrame): 
    """
    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date   = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # Request reports
    report_ids = {}
    current_date = start_date
    while current_date <= end_date:
        try:
            report_id = request_report(current_date, current_date, marketplace, asin_granularity)
            report_ids[current_date] = report_id
            current_date += dt.timedelta(days=1)
            time.sleep(5)

        except Exception as error:
            logger.warning(f"Error: {error}")
            time.sleep(60)

    # Downloads & combines daily reports
    combined_data = pd.DataFrame()
    for date in report_ids:
        report_id = report_ids[date]
        document_id = get_report(report_id)
        data = download_report(document_id)

        # Combines data
        df = pd.json_normalize(data, sep='_')
        df['date'] = date
        df['marketplace'] = marketplace
        combined_data = pd.concat([df, combined_data], ignore_index=True)

    return combined_data


def update_data(asin_granularity='PARENT', marketplaces=['US', 'CA', 'UK'], start_date=(dt.datetime.utcnow() - dt.timedelta(days=7)), end_date=(dt.datetime.utcnow() - dt.timedelta(days=1))):
    for marketplace in to_list(marketplaces):
        logger.info(f"Updating data {asin_granularity} {marketplace} {start_date} - {end_date}")
        data = download_combine_reports(start_date, end_date, marketplace, asin_granularity=asin_granularity)
        table_name = base_table_name + f"_{asin_granularity.lower()}"
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(asin_granularity, drop_table_if_exists=False):
    # Requests worth 30 days sample of data
    start_date = (dt.datetime.utcnow() - dt.timedelta(days=5))
    end_date = (dt.datetime.utcnow() - dt.timedelta(days=1))
    data = download_combine_reports(start_date, end_date, 'US', asin_granularity=asin_granularity)
    
    table_name = base_table_name + f"_{asin_granularity.lower()}"

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name)

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    # update_data('PARENT') 
    # update_data('CHILD')

    # Create a date range
    start_date = dt.date(2023, 7, 1)
    end_date = dt.date.today() - dt.timedelta(days=2)
    date_range = pd.date_range(start_date, end_date)

    # Find unique months in the date range
    unique_months = date_range.to_period('M').unique()
    
    tasks = []
    for marketplace in ['UK']:
        for month in unique_months:
            start_of_month = month.to_timestamp(how='start')
            end_of_month = month.to_timestamp(how='end')
            logger.info(f"Updating for {start_of_month} - {end_of_month}")
            update_data('PARENT', marketplace, start_date=start_of_month, end_date=end_of_month)
            # update_data('CHILD', marketplace, start_date=start_of_month, end_date=end_of_month)
