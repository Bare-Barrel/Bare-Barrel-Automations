import pandas as pd
import numpy as np
import datetime as dt
import os
from postgresql import setup_cursor, sql_standardize
import re
import psycopg2
import csv
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

def get_amazon_products(country : str = 'US') -> list:
    """Retrieves active amazon products from product_amazon db
    Args:
    country = 'US', 'CA', 'UK'
    Return list of ASINs"""
    cur = setup_cursor('ppc').connect()
    cur.execute("""SELECT asin FROM product_amazon
                    WHERE active IS TRUE;""")
    asins = [row['asin'] for row in cur.fetchall()]
    return asins


def end_of_week_date(date : dt.date) -> dt.date:
    """Returns the date of end of the week (Saturday)"""
    start_date = date - dt.timedelta(days=date.weekday()) # Sunday's date
    end_date   = start_date + dt.timedelta(days=5) # Saturday's date
    return end_date


def is_utf8(s):
    try:
        s.encode('utf-8')
        return True
    except UnicodeEncodeError:
        return False


def insert_sqp_reports(csv_path : str) -> None:
    """Insert Search Query Performance Reports to db"""
    cur = setup_cursor().connect()
    filename = os.path.basename(csv_path)
    metadata = pd.read_csv(csv_path, nrows=0)
    data     = pd.read_csv(csv_path, skiprows=1)
    # extracts info from filename
    country = filename.split('_')[0]
    view = 'brand' if 'brand' in filename.lower() else 'asin'
    table_name = 'brand_analytics.search_query_performance_{}_view'.format(view)
    logger.info(f"\tINSERTING {metadata.columns} to {table_name}")
    # cleans & inserts metadata
    data['country'] = country
    for col in metadata.columns:
        splitted_col = col.split('=')
        value = re.sub(r'\W', '', splitted_col[1])
        if re.search('ASIN', col, re.IGNORECASE):
            data['asin'] = value
        elif re.search('Reporting Range', col, re.IGNORECASE):
            data['reporting_range'] = value
    # cleans columns to match database naming convention
    data.columns = [sql_standardize(col, remove_parenthesis=False) for col in data.columns]
    # calculates week number, start & end date
    data['reporting_date'] = pd.to_datetime(data['reporting_date'])
    data['end_date']       = data['reporting_date']
    data['start_date']     = data['end_date'] - dt.timedelta(days=6)
    data['week']           = data['end_date'].dt.isocalendar().week
    data['created']        = dt.datetime.now()
    # arranges column name following db column order
    cur.execute("""SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position;""", (table_name, ))
    column_names = [row['column_name'] for row in cur.fetchall()]
    data = data[column_names]
    logger.info(data.head(3))
    # removing & fixing `tab spaces` in search query
    data['search_query'] = data['search_query'].replace('    ', ' ', regex=True)
    data['search_query'] = data['search_query'].replace(r'\\', '(?)', regex=True)
    # inserts to db
    temp_csv = os.path.join(os.getcwd(), 'sqp_temp.csv')
    data.to_csv(temp_csv, index=False, sep='\t')
    with open(temp_csv, 'r', encoding='utf-8') as file:
        next(file)
        # cur.execute(f"""COPY {table_name} FROM '{temp_csv}' DELIMITER ',' CSV HEADER;""")
        cur.copy_from(file, table_name, null='', sep='\t', columns=(col for col in data.columns))
    cur.close()
    return


def raw_insert_ppc_reports(sponsored_type):
    RAW_folder = '/mnt/c/Users/Calvin/OneDrive/Saratoga Home/PPC Data Review/RAW'
    for path, currentDirectory, files in os.walk(RAW_folder):
        # Skipping 2019-2020
        if '2019' in path or '2020' in path or '2021' in path:
            logger.info(f'skipping {path}')
            continue
        for file in files:
            if 'products' in file.lower():
                filepath = os.path.join(path, file)
                logger.info(f'#Inserting {filepath}')
                insert_ppc_reports(filepath, 'sponsored_products')


def insert_ppc_reports(excel_path : str, sponsored_type : str):
    """Converts sponsored performance reports to CSV and then inserts into db
    
    Args:
        excel_path (str|os.path): Path to Sponsored Report
        sponsored_type (str): ['sponsored_product', 'sponsored_brand', 'sponsored_display']

    Return: None?"""
    table_name = sponsored_type + '_amazon'
    cur = setup_cursor('ppc').connect()
    cur.execute(f"SELECT column_details FROM metadata WHERE table_name = '{table_name}';")
    column_details = cur.fetchone()['column_details']
    column_names = [column_details[col] for col in column_details]
    data = pd.read_excel(excel_path)
    data.columns = data.columns.str.strip()
    # US & CA have slight differences in column names
    data.rename(columns={'Portfolio name': 'Portfolio Name', '7 Day Total Sales ($)': '7 Day Total Sales', 
        'Advertising Cost of Sales (ACOS)': 'Total Advertising Cost of Sales (ACOS)', 'Return on Advertising Spend (ROAS)': 'Total Return on Advertising Spend (ROAS)',
        '7 Day Advertised SKU Sales ($)': '7 Day Advertised SKU Sales', '7 Day Other SKU Sales ($)': '7 Day Other SKU Sales'}, inplace=True)
    data = data[column_names]
    data['created'] = dt.datetime.now()
    temp_csv = os.path.join(os.getcwd(), 'ppc_data_temp.csv')
    data.to_csv(temp_csv, index=False)
    logger.info(f"Inserting into {table_name} \n{data.head(2)}") 
    try:
        cur.execute(f"""COPY {table_name} FROM '{temp_csv}' DELIMITER ',' CSV HEADER;""")
    except Exception as e:
        logger.error(e)

if __name__ == '__main__':
    # filepath = os.path.join('SQP Downloads', 'CA_Search_Query_Performance_Brand_View_Simple_Week_2023_02_18.csv')
    # insert_sqp_reports(filepath)
    filepath = os.path.join('PPC Data', 'Sponsored Products Search term report.xlsx')
    insert_ppc_reports(filepath, 'sponsored_product')
    pass