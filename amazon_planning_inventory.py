import datetime as dt
from sp_api.base.reportTypes import ReportType
from sp_api.base import Marketplaces
from utility import to_list, reposition_columns
from amazon_reports import request_report, get_report, download_report
import time
import pandas as pd
import postgresql
import logging
import logger_setup
from io import StringIO

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_name = 'inventory.fba_planning_inventory'


def download_combine_reports(marketplaces=['US', 'CA', 'UK']):
    """
    Requests GET_FBA_INVENTORY_PLANNING_DATA and downloads daily reports. 
    It can only request data at t-2. 
    Parsed the tab delimited text file. 
    Combines reports and returns pandas dataframe.

    Args:
        marketplace (str): 'US', 'CA', 'UK'

    Returns:
        data (pd.DataFrame)
    """
    # Requests report
    report_ids = {}
    for marketplace in to_list(marketplaces):

        for tries in range(0, 21):
            try:
                report_id = request_report(ReportType.GET_FBA_INVENTORY_PLANNING_DATA, marketplace)
                report_ids[marketplace] = report_id
                break

            except Exception as error:
                if tries == 20:
                    logger.error(error)
                    raise error
                logger.warning(f"Error: {error}")
                time.sleep(60)

    # Combines report
    combined_data = pd.DataFrame()

    for marketplace in report_ids:
        # Downloads report in bytes
        report_id = report_ids[marketplace]
        document_id = get_report(report_id, marketplace)
        downloaded_data = download_report(document_id, marketplace) # bytes of the text data

        # Decode the bytes into a string & split string into lines
        str_downloaded_data = downloaded_data.decode('utf-8')
        data_lines = str_downloaded_data.strip().split('\n')

        # Reads data to dataframe
        data = pd.read_csv(StringIO('\n'.join(data_lines)), delimiter='\t')
        data = reposition_columns(data, {'marketplace': 0})
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    return combined_data


def update_data(marketplaces=['US', 'CA', 'UK']):
    """
    Updates GET_FBA_INVENTORY_PLANNING_DATA latest snapshot at t-2
    """
    logger.info(f"Updating GET_FBA_INVENTORY_PLANNING_DATA {marketplaces}")
    data = download_combine_reports(marketplaces)
    postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(drop_table_if_exists=True):
    # Requests worth 30 days sample of data
    data = download_combine_reports()

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name,
                            keys='PRIMARY KEY (marketplace, snapshot_date, sku, condition)')

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


if __name__ == '__main__':
    update_data()