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


def request_report(report_type, account, marketplace, start_date=None, end_date=None, **kwargs):
    """
    Requests any type of report found in 
    https://developer-docs.amazon.com/sp-api/docs/sp-api-seller-use-cases#fulfillment-by-amazon-fba

    Args:
        report_type (sp_api.base.reportTypes): ReportType
        start_date, end_date (str | dt.date): YYYY-MM-DD
        marketplace (str): 'US', 'CA', 'UK'
        **kwargs = reportOptions

    Returns:
        report_id (str)
    """
    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
        end_date   = dt.datetime.strptime(end_date, '%Y-%m-%d')

    # Sets start date (00:00:00) and end date (23:59:59)
    if start_date and end_date:
        start_date = dt.datetime.combine(start_date, dt.time.min).isoformat()
        end_date = dt.datetime.combine(end_date, dt.time.max).isoformat()

    logger.info(f"Requesting {report_type} {account}-{marketplace} {f'{start_date} - {end_date}' if start_date else ''}")

    response = ReportsV2(
                    account=f'{account}-{marketplace}',
                    marketplace=Marketplaces[marketplace]
            ).create_report(
                        reportType = report_type,
                        reportOptions = kwargs,
                        # optionally, you can set a start and end time for some reports
                        dataStartTime = start_date,
                        dataEndTime = end_date
    )

    report_id = response.payload['reportId']

    return report_id


def get_report(report_id, account, marketplace):
    """
    Checks and waits for the report status to be downloaded.

    Returns document_id (str)
    """
    result = ReportsV2(account=f'{account}-{marketplace}', 
                        marketplace=Marketplaces[marketplace]).get_report(report_id)
    payload = result.payload
    status = payload['processingStatus']
    logger.info(f"Report Processing Status: {status}")

    if payload['processingStatus'] == 'DONE':
        document_id = result.payload['reportDocumentId']
        if document_id:
            
            return document_id

    elif payload['processingStatus'] in ('CANCELLED','FATAL', 'FAILED'):
        logger.warning(f"Report {report_id} was {payload['processingStatus'].lower()}.")
        return None

    time.sleep(15)
    return get_report(report_id, account, marketplace)


def download_report(document_id, account, marketplace):
    """
    Downloads the url from the `get_report` function into memory without saving the file.

    Returns response.content
    """
    try:
        response = ReportsV2(account=f'{account}-{marketplace}', 
                                marketplace=Marketplaces[marketplace]).get_report_document(document_id)
        url = response.payload['url']
        response = requests.get(url)

        if response.status_code == 200:
            logger.info("\tFile downloaded successfully.")

            return response.content

        time.sleep(1)

    # Retries
    except Exception as error:
        logger.warning(f"Error {error}")
        time.sleep(30)
        return download_report(document_id, account, marketplace)


if __name__ == '__main__':
    pass