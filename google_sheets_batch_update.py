import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
from google_sheets_data_sources_query import worksheet_queries
import postgresql
import time
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

tenants = postgresql.get_tenants()

# Google Sheets parameters
google_sheet_data_sources = {
    'Bare Barrel':
    {
        '[KPIs][K1] WW Data Sources': 'https://docs.google.com/spreadsheets/d/1-DKSsS0yA8tFDHBeOawVQp4nczCjmNPNJrsqyjgR4bc/edit?gid=732099413#gid=732099413',
        '[Supply C.][SC] WW Data Sources': 'https://docs.google.com/spreadsheets/d/16TKSUWBkJ4MAebOXt33RRWR3NA9gSzzQIOEdPHtRQno/edit?gid=2126659402#gid=2126659402'
    },
    'Rymora':
    {
        '[KPIs][K1][Rymora] WW Data Sources': 'https://docs.google.com/spreadsheets/d/1-IEyNY2y5sjr-90TEhg1riOTPloRRwOa9-S22g766-o/edit?gid=1803521062#gid=1803521062',
        '[Supply C.][SC][Rymora] WW Data Sources': 'https://docs.google.com/spreadsheets/d/1shyRI3zAY9avDWvbrgD_JEe2bwRpFUQZJ1YwfPUDvW8/edit?gid=549226539#gid=549226539'

    }
}

# Sheets start row & col
google_sheets_custom_row_col = {
    'Orders-US': [3, 1],
    'Orders-CA': [3, 1],
    'Orders-UK': [3, 1]
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


def batch_update_data_sources(url, worksheets='All', account='Bare Barrel'):
    '''
    Updates google sheets data sources by specifying url.
    It automatically detects worksheets to be updated by checking the sheet name in the `worksheet_queries`.

    #IMPORTANT#!
    It requires to have a Google project in the Google Cloud Platform and share the sheet to the google bot as an editor.

    Args
        url (str): url of the Google Sheet
        sheets (str, list): Worksheets to be updated
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
    if worksheets == 'All':
        worksheet_names = [worksheet.title for worksheet in sheet.worksheets() if worksheet.title in worksheet_queries]
    elif isinstance(worksheets, str):
        worksheet_names = [worksheets]
    elif isinstance(worksheets, list):
        worksheet_names = worksheets

    # Batch Update Google Sheet
    for worksheet_name in worksheet_names:
        try:
            logger.info(f"\nOpening {account} worksheet {worksheet_name}")
            worksheet = sheet.worksheet(worksheet_name)

            logger.info("\tGetting data from database...")
            tenant_id = str(tenants[account])
            query = worksheet_queries[worksheet_name]
            # tenant_id = f"({', '.join(repr(v) for v in tenant_id)},)" # makes sure it ends with trailing comma
            # query = query % tenant_id
            df = postgresql.sql_to_dataframe(query.replace('%s', tenant_id))



            logger.info('\tUpdating worksheet data...')
            if worksheet_name in google_sheets_custom_row_col:
                set_with_dataframe(worksheet, df, 
                                   row=google_sheets_custom_row_col[worksheet_name][0], 
                                   col=google_sheets_custom_row_col[worksheet_name][1])
            else:
                set_with_dataframe(worksheet, df)

            logger.info('\tSuccess!')
            time.sleep(2.5)


        except Exception as e:
            logger.error(e)
            raise e
        

if __name__ == '__main__':
    # batch_update_data_sources(google_sheet_data_sources['Rymora']['[Supply C.][SC][Rymora] WW Data Sources'], 'FBA-Inv')
    
    for account in tenants.keys():

        for google_sheet in google_sheet_data_sources[account]:

            sheet_url = google_sheet_data_sources[account][google_sheet]

            batch_update_data_sources(sheet_url, account=account)