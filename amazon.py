import pandas as pd
import numpy as np
import datetime as dt
import os
from postgresql import setup_cursor
import re
import psycopg2

def get_amazon_products(country : str = 'US') -> list:
    """Retrieves active amazon products from product_amazon db
    Args:
    country = 'US', 'CA', 'UK'
    Return list of ASINs"""
    cur = setup_cursor()
    cur.execute("""SELECT asin FROM product_amazon
                    WHERE active IS TRUE;""")
    asins = [row['asin'] for row in cur.fetchall()]
    return asins


def end_of_week_date(date : dt.date) -> dt.date:
    """Returns the date of end of the week (Saturday)"""
    start_date = date - dt.timedelta(days=date.weekday()) # Sunday's date
    end_date   = start_date + dt.timedelta(days=5) # Saturday's date
    return end_date


def insert_sqp_reports(csv : str, country : str) -> None:
    """Insert Search Query Performance Reports to db"""
    cur = setup_cursor()
    metadata = pd.read_csv(csv, nrows=0)
    data     = pd.read_csv(csv, skiprows=1)
    table_name = 'search_query_performance'
    print(f"\tINSERTING {metadata.columns} to {table_name}")
    # cleans & inserts metadata
    data['country'] = country
    for col in metadata.columns:
        splitted_col = col.split('=')
        value = re.sub(r'\W', '', splitted_col[1])
        if re.search('ASIN', col, re.IGNORECASE):
            data['asin'] = value
        elif re.search('Reporting Range', col, re.IGNORECASE):
            data['reporting_range'] = value
    # calculates week number, start & end date
    data['Reporting Date'] = pd.to_datetime(data['Reporting Date'])
    data['end_date']       = data['Reporting Date']
    data['start_date']     = data['end_date'] - dt.timedelta(days=6)
    data['week']           = data['end_date'].dt.isocalendar().week
    data['created']        = dt.datetime.now()
    # arranges column name following db column order
    cur.execute("""SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position;""", (table_name, ))
    column_names = [row['column_name'] for row in cur.fetchall()]
    data = data[column_names]
    print(data.head(3))
    # inserts to db
    temp_csv = os.path.join(os.getcwd(), 'sqp_temp.csv')
    data.to_csv(temp_csv, index=False)
    cur.execute(f"""COPY {table_name} FROM '{temp_csv}' DELIMITER ',' CSV HEADER;""")
    cur.close()
    return