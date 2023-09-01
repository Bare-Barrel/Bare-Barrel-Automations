from playwright.sync_api import Playwright, sync_playwright, expect
from playwright.async_api import async_playwright
import json
import pandas as pd
import numpy as np
import datetime as dt
import re
import time
import asyncio
import os
import re
import shutil
import postgresql
from utility import get_day_of_week, reposition_columns
from playwright_setup import setup_playwright, login_amazon
from tzlocal import get_localzone
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)


file_path = os.path.join(os.getenv('HOME'), 'Downloads', 'helium10-kt-B0BWWY7H3F-2023-08-28.csv')

table_name = 'rankings.h10_keyword_tracker'

data = pd.read_csv(file_path)
data = data.replace({'-': np.nan, '>306': 306})
numeric_cols = ['Search Volume', 'Organic Rank', 'Sponsored Position']
data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric)
data['Date Added'] = pd.to_datetime(data['Date Added']).dt.tz_localize(get_localzone())
data['Date Added'] = data['Date Added'].dt.tz_convert('UTC')


drop_table_if_exists = True

with postgresql.setup_cursor() as cur:
    if drop_table_if_exists:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    primary_keys = 'PRIMARY KEY (asin, marketplace, keyword, date_added)'

    postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name, keys=primary_keys)

    postgresql.update_updated_at_trigger(cur, table_name)

    postgresql.upsert_bulk(table_name, data, file_extension='pandas')