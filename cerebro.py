import pandas as pd
import numpy as np
import datetime as dt
import os
from postgresql import setup_cursor
import psycopg2
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

def insert_data(csv : str, **kwargs) -> None:
    """Cleans & insert downloaded cerebro csv file according to database structure
    **kwargs - additional columns"""
    metadata = "{} ({}) - {} - {} - {}".format(kwargs['platform'], kwargs['country'], kwargs['asin'], kwargs['category'], kwargs['date'])
    logger.info(f"\tINSERTING {metadata}")
    cur = setup_cursor().connect('ppc')
    table_name = f"cerebro_{kwargs['platform']}"
    # removes competitors' organic ranking
    data = pd.read_csv(csv)
    data = data.loc[:, :'Competitor Performance Score']
    # adds columns
    for col in kwargs.keys():
        data[col] = kwargs[col]
    logger.info(data.head(5))
    data['created'] = dt.datetime.now()
    data.replace('-', '', inplace=True)
    data.dropna(subset='Competitor Performance Score', inplace=True) # removes 0 competitor performance score
    # arranges column name following db column order
    cur.execute("""SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position;""", (table_name, ))
    column_names = [row['column_name'] for row in cur.fetchall()]
    # adding null values on missing columns in other marketplaces
    missing_cols = [col for col in column_names if col not in data.columns]
    if missing_cols:
        data[missing_cols] = None
    data = data[column_names]
    # inserts to db
    file = 'cerebro_temp.csv'
    temp_csv = os.path.join(os.getcwd(), file)
    data.to_csv(temp_csv, index=False)
    try:
        logger.info(f"\tCopying {file} to {table_name}")
        cur.execute(f"""COPY {table_name} FROM '{temp_csv}' DELIMITER ',' CSV HEADER;""")
    except psycopg2.errors.UniqueViolation as error:
        logger.error(error)
    cur.close()
    return


if __name__ == '__main__':
    csv = os.path.join(os.getcwd(), 'US_AMAZON_cerebro_B08611LCC7_2023-01-12.csv')
    insert_data(csv, country='US', category='Canisters', asin='B08611LCC7',
                                        date=dt.datetime.now().date(), platform='amazon')