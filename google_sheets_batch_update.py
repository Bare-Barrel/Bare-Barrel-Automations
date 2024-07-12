import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
from google_sheets_data_sources_query import worksheet_queries
from postgresql import sql_to_dataframe
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)


# Google Sheets parameters
google_sheet_name = '[KPIs][K1] WW Data Sources'
google_sheet_url = 'https://docs.google.com/spreadsheets/d/1-DKSsS0yA8tFDHBeOawVQp4nczCjmNPNJrsqyjgR4bc/edit?gid=732099413#gid=732099413'

# Google Sheets API authentication
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

credentials = Credentials.from_service_account_file(
    'google_sheets_service_account.json',
    scopes=scopes
)

logger.info("Authorizing Google Sheets")
gc = gspread.authorize(credentials)

# Open Google Sheet
logger.info(f"Opening URL {google_sheet_url}")
sheet = gc.open_by_url(google_sheet_url) # Make sure to share bot's email in the Google Sheets
logger.info(sheet)

# Batch Update Google Sheet
for worksheet_name in worksheet_queries:
    try:
        logger.info(f"\nOpening worksheet {worksheet_name}")
        worksheet = sheet.worksheet(worksheet_name)

        logger.info("\tGetting data from database...")
        df = sql_to_dataframe(worksheet_queries[worksheet_name])

        logger.info('\tUpdating worksheet data...')
        set_with_dataframe(worksheet, df, row=3) # Coefficient occupies first 2 rows

        logger.info('\tSuccess!')

    except Exception as e:
        logger.error(e)
        raise e