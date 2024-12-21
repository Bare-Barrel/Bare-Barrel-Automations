import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
from google_sheets_data_sources_query import worksheet_queries
from postgresql import sql_to_dataframe
import time
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)


# Google Sheets parameters
google_sheet_data_sources = {
    '[KPIs][K1] WW Data Sources': 'https://docs.google.com/spreadsheets/d/1-DKSsS0yA8tFDHBeOawVQp4nczCjmNPNJrsqyjgR4bc/edit?gid=732099413#gid=732099413',
    '[Supply C.][SC] WW Data Sources': 'https://docs.google.com/spreadsheets/d/16TKSUWBkJ4MAebOXt33RRWR3NA9gSzzQIOEdPHtRQno/edit?gid=2126659402#gid=2126659402'
}


# Google Sheets API authentication
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

credentials = Credentials.from_service_account_file(
    'google_sheets_service_account.json',
    scopes=scopes
)


def batch_update_data_sources(url):
    '''
    Updates google sheets data sources by specifying url.
    It automatically detects worksheets to be updated by checking the sheet name in the `worksheet_queries`.

    #IMPORTANT#!
    It requires to have a Google project in the Google Cloud Platform and share the sheet to the google bot as an editor.

    Args
        url (str): url of the Google Sheet
    Returns
        None
    '''
    logger.info("Authorizing Google Sheets")
    gc = gspread.authorize(credentials)

    # Open Google Sheet
    logger.info(f"Opening URL {url}")
    sheet = gc.open_by_url(url)
    logger.info(sheet)

    # Gets worksheet names to be updated
    worksheet_names = [worksheet.title for worksheet in sheet.worksheets() if worksheet.title in worksheet_queries]

    # Batch Update Google Sheet
    for worksheet_name in worksheet_names:
        try:
            logger.info(f"\nOpening worksheet {worksheet_name}")
            worksheet = sheet.worksheet(worksheet_name)

            logger.info("\tGetting data from database...")
            df = sql_to_dataframe(worksheet_queries[worksheet_name])

            logger.info('\tUpdating worksheet data...')
            set_with_dataframe(worksheet, df)

            logger.info('\tSuccess!')
            time.sleep(2.5)


        except Exception as e:
            logger.error(e)
            raise e
        

if __name__ == '__main__':

    for google_sheet in google_sheet_data_sources:

        sheet_url = google_sheet_data_sources[google_sheet]

        batch_update_data_sources(sheet_url)