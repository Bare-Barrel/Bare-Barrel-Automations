import datetime as dt
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.base import Marketplaces
import time
import pandas as pd
import postgresql
import requests
import gzip
import json


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

    print(f"Requesting download Sales & Traffic Report ({marketplace}) {asin_granularity} {str(start_date)} - {str(end_date)}")

    response = ReportsV2(marketplace=Marketplaces[marketplace]).create_report(
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
        end_date   = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # Request reports
    report_ids = {}
    current_date = start_date
    while current_date <= end_date:
        try:
            report_id = request_report(current_date, current_date, marketplace, asin_granularity)
            report_ids[current_date] = report_id
            current_date += dt.timedelta(days=1)
            time.sleep(10)

        except Exception as error:
            print(f"Error: {error}")
            time.sleep(60)

    # Downloads & combines daily reports
    data = pd.DataFrame()
    for date in report_ids:
        # Waits until report ready to be download
        while True:
            result = ReportsV2().get_report(report_id)
            payload = result.payload
            status = payload['processingStatus']
            print(f"Report Processing Status: {status}")

            if payload['processingStatus'] == 'DONE':
                document_id = result.payload['reportDocumentId']
                break

            time.sleep(15)

        # Downloads reports
        while True:
            try:
                response = ReportsV2().get_report_document(document_id)
                url = response.payload['url']
                response = requests.get(url)
                time.sleep(1)
                break
            except Exception as error:
                print(f"Error {error}")
                time.sleep(30)

        if response.status_code == 200:
            print("\tFile downloaded successfully.")
            compressed_data = response.content

            # Decompresses data
            decompressed_data = gzip.decompress(compressed_data).decode('utf-8')

            # Parse salesAndTrafficByAsin
            sales_and_traffic = json.loads(decompressed_data)['salesAndTrafficByAsin'] # list of dicts

            # Combines data
            df = pd.json_normalize(sales_and_traffic, sep='_')
            df['date'] = date
            df['marketplace'] = marketplace
            data = pd.concat([df, data], ignore_index=True)


    return data


def update_data(asin_granularity='PARENT', marketplace='US', start_date=(dt.datetime.utcnow() - dt.timedelta(days=10)), end_date=(dt.datetime.utcnow() - dt.timedelta(days=1))):
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
    # create_table('PARENT', drop_table_if_exists=False)
    # while True:
    # with postgresql.setup_cursor() as cur:
        # # cur.execute("SELECT MAX(date) FROM business_reports.detail_page_sales_and_traffic_parent;")
        # # end_date = cur.fetchone()['max'] - dt.timedelta(days=1)
        # end_date = dt.date.today()
        # start_date = end_date - dt.timedelta(days=10)
        # update_data(start_date=start_date, end_date=end_date)
        # time.sleep(5)
    update_data()