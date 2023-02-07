import psycopg2
import psycopg2.extras
import json
import os
import pandas as pd

with open("config.json") as f:
    config = json.load(f)


def setup_cursor():
    # Establishing connection to database (Make sure to sudo service postgresql start)
    conn = psycopg2.connect(f"dbname={config['postgres_db']} user={config['postgres_user']} host={config['postgres_host']} port={config['postgres_port']} password={config['postgres_password']}")
    conn.set_session(autocommit = True)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)      # https://www.psycopg.org/docs/extras.html#real-dictionary-cursor
    return cur


def sql_to_dataframe(query):
   """
   Import data from a PostgreSQL database using a SELECT query 
   """
   try:
        conn = psycopg2.connect(f"dbname={config['postgres_db']} user={config['postgres_user']} host={config['postgres_host']} port={config['postgres_port']} password={config['postgres_password']}")
        cur = conn.cursor()
        cur.execute(query)
   except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cur.close()
        return None
   # The execute returns a list of tuples:
   tuples_list = cur.fetchall()
   column_names = [column[0] for column in cur.description]
   cur.close()
   # Now we need to transform the list into a pandas DataFrame:
   df = pd.DataFrame(tuples_list, columns=column_names)
   return df


def create_cerebro_amazon(cur):
    # "ABA SFR" DECIMAL removed from cerebro
    cur.execute("""
        CREATE TABLE cerebro_amazon (
            country VARCHAR(5),
            category VARCHAR(25),
            asin VARCHAR(15),
            date DATE,
            "Keyword Phrase" VARCHAR(150),
            "ABA Total Click Share" NUMERIC(4,1),
            "ABA Total Conv. Share" NUMERIC(4,1), 
            "Keyword Sales" INT,
            "Cerebro IQ Score" INT,
            "Search Volume" INT,
            "Search Volume Trend" SMALLINT,
            "H10 PPC Sugg. Bid" NUMERIC(5,2),
            "H10 PPC Sugg. Min Bid" NUMERIC(5,2),
            "H10 PPC Sugg. Max Bid" NUMERIC(5,2),
            "Sponsored ASINs" SMALLINT,
            "Competing Products" INT,
            "CPR"  SMALLINT,
            "Title Density"  SMALLINT,
            "Amazon Recommended"  SMALLINT,
            "Sponsored"  SMALLINT,
            "Organic"  SMALLINT,
            "Sponsored Rank (avg)"  SMALLINT,
            "Sponsored Rank (count)"  SMALLINT,
            "Amazon Recommended Rank (avg)" SMALLINT,
            "Amazon Recommended Rank (count)" SMALLINT,
            "Position (Rank)" SMALLINT,
            "Relative Rank" SMALLINT,
            "Competitor Rank (avg)" NUMERIC(4,1),
            "Ranking Competitors (count)" SMALLINT,
            "Competitor Performance Score" NUMERIC(4,1),
            created TIMESTAMP WITH TIME ZONE DEFAULT now(),
            PRIMARY KEY (country, category, date, "Keyword Phrase")
        );
    """)
    return


def create_product_amazon(cur):
    cur.execute("""
        CREATE TABLE product_amazon (
            asin VARCHAR(15) PRIMARY KEY,
            product_name VARCHAR(30) NOT NULL,
            category VARCHAR(30) NOT NULL,
            active BOOLEAN
        );
    """)


def create_search_query_performance(cur):
    cur.execute("""
        CREATE TABLE search_query_performance (
            country VARCHAR(3) NOT NULL,
            asin VARCHAR(10) NOT NULL,
            reporting_range VARCHAR(6) NOT NULL,
            week SMALLINT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            "Search Query" VARCHAR(250),
            "Search Query Score" SMALLINT,
            "Search Query Volume" INT,
            "Impressions: Total Count" INT,
            "Impressions: ASIN Count" SMALLINT,
            "Impressions: ASIN Share %" NUMERIC(5,2),
            "Clicks: Total Count" INT,
            "Clicks: Click Rate %" NUMERIC(6,2),
            "Clicks: ASIN Count" SMALLINT,
            "Clicks: ASIN Share %" NUMERIC(5,2),
            "Clicks: Price (Median)" NUMERIC(6,2),
            "Clicks: ASIN Price (Median)" NUMERIC(6,2),
            "Clicks: Same Day Shipping Speed" SMALLINT,
            "Clicks: 1D Shipping Speed" SMALLINT,
            "Clicks: 2D Shipping Speed" SMALLINT,
            "Cart Adds: Total Count" SMALLINT,
            "Cart Adds: Cart Add Rate %" NUMERIC(6,2),
            "Cart Adds: ASIN Count" SMALLINT,
            "Cart Adds: ASIN Share %" NUMERIC(5,2),
            "Cart Adds: Price (Median)" NUMERIC(6,2),
            "Cart Adds: ASIN Price (Median)" NUMERIC(6,2),
            "Cart Adds: Same Day Shipping Speed" SMALLINT,
            "Cart Adds: 1D Shipping Speed" SMALLINT,
            "Cart Adds: 2D Shipping Speed" SMALLINT,
            "Purchases: Total Count" SMALLINT,
            "Purchases: Purchase Rate %" NUMERIC(5,2),
            "Purchases: ASIN Count" SMALLINT,
            "Purchases: ASIN Share %" NUMERIC(5,2),
            "Purchases: Price (Median)" NUMERIC(6,2),
            "Purchases: ASIN Price (Median)" NUMERIC(6,2),
            "Purchases: Same Day Shipping Speed" SMALLINT,
            "Purchases: 1D Shipping Speed" SMALLINT,
            "Purchases: 2D Shipping Speed" SMALLINT,
            "Reporting Date" DATE,
            created TIMESTAMP WITH TIME ZONE DEFAULT now(),
            PRIMARY KEY (country, asin, reporting_range, "Reporting Date", "Search Query")
        );""")


if __name__ == "__main__":
    cur = setup_cursor()
    create_search_query_performance(cur)
    pass