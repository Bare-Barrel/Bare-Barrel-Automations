import os
import time
import postgresql
import logging
import logger_setup
import pandas as pd
import numpy as np
import datetime as dt
from zoneinfo import ZoneInfo
from utility import sync_with_rclone


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_name = 'rankings.h10_keyword_tracker'


def clean_data(file_path):
    data = pd.read_csv(file_path)
    data = data.drop_duplicates()
    data = data.replace({'-': np.nan})
    numeric_cols = ['Search Volume', 'Organic Rank', 'Sponsored Position']
    data[numeric_cols] = data[numeric_cols].applymap(
        lambda x: int(str(x).replace(">", "").replace("<", ""))
        if pd.notna(x)
        else np.nan
    )
    data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric)
    # The dates in the files are in local timezone, so we need to convert it to UTC
    timezone = ZoneInfo('Asia/Manila')
    data['Date Added'] = pd.to_datetime(data['Date Added']).dt.tz_localize(timezone)
    data['Date Added'] = data['Date Added'].dt.tz_convert('UTC')
    return data


def update_data(file_path):
    data = clean_data(file_path)
    data = data[data['Organic Rank'].notnull()]
    postgresql.upsert_bulk(table_name, data, 'pandas')


"""
The following should be added as environment variables:
export Automations="/usr/local/bin/Bare-Barrel-Automations"
export RCLONE_CONFIG=$Automations/"rclone.conf"
"""
# Syncs Google Drive to Remote Folder
google_drive_source = "google_drive:H10 Keyword Tracker Downloads"
destination_folder = os.path.join(
    os.getenv('Automations'), 'H10 Keyword Tracker Downloads'
)

if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)
    logger.info(f"Created directory: {destination_folder}")

sync_with_rclone(google_drive_source, destination_folder, config_path='rclone.conf')

# Checks metadata file mofication time
stat_info = os.stat(destination_folder)

# Convert epoch time to datetime
modification_time = dt.datetime.fromtimestamp(stat_info.st_mtime)

# Compares database last updated date to the metadata file created date
query = f"""SELECT MAX(updated_at) 
            FROM {table_name};"""

max_updated_date = postgresql.sql_to_dataframe(query)['max'][0]

logger.info(f"Database's latest created date: {max_updated_date}")
logger.info(f"Metadata file's created date: {modification_time}")

if max_updated_date < modification_time:
    logger.info("New data found in metadata file. Updating database...")

    # Update database
    for file in os.listdir(destination_folder):
        if file.endswith('.csv'):
            filepath = os.path.join(destination_folder, file)
            logger.info(f"\tUpdating database with file: {file}")
            update_data(filepath)

    logger.info("\tDatabase updated successfully!")

else:
    logger.info("No new data found in metadata file. Skipping database update...")
