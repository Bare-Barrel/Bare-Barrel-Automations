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


def sql_to_dataframe(query, vars=None):
   """
   Import data from a PostgreSQL database using a SELECT query 
   """
   try:
        conn = psycopg2.connect(f"dbname={config['postgres_db']} user={config['postgres_user']} host={config['postgres_host']} port={config['postgres_port']} password={config['postgres_password']}")
        cur = conn.cursor()
        cur.execute(query, vars)
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


def create_metadata(cur):
    "Metadata of column names"
    cur.execute("""
        CREATE TABLE metadata (
            table_name TEXT PRIMARY KEY,
            created TIMESTAMP,
            column_details JSON
        );""")


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
            product_name VARCHAR(200) NOT NULL,
            product_code VARCHAR(10),
            sku VARCHAR(50) NOT NULL,
            category VARCHAR(30) NOT NULL,
            active BOOLEAN
        );
    """)


def create_search_query_performance_asin_view(cur):
    cur.execute("""
                    CREATE TABLE search_query_performance_asin_view (
                        country VARCHAR(3) NOT NULL,
                        asin VARCHAR(10) NOT NULL,
                        reporting_range VARCHAR(6) NOT NULL,
                        week INT NOT NULL,
                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,
                        search_query VARCHAR(310),
                        search_query_score INT,
                        search_query_volume INT,
                        impressions_total_count INT,
                        impressions_asin_count INT,
                        impressions_asin_share NUMERIC(8,2),
                        clicks_total_count INT,
                        clicks_click_rate NUMERIC(8,2),
                        clicks_asin_count INT,
                        clicks_asin_share NUMERIC(8,2),
                        clicks_price_median NUMERIC(8,2),
                        clicks_asin_price_median NUMERIC(8,2),
                        clicks_same_day_shipping_speed INT,
                        clicks_1d_shipping_speed INT,
                        clicks_2d_shipping_speed INT,
                        cart_adds_total_count INT,
                        cart_adds_cart_add_rate NUMERIC(8,2),
                        cart_adds_asin_count INT,
                        cart_adds_asin_share NUMERIC(8,2),
                        cart_adds_price_median NUMERIC(8,2),
                        cart_adds_asin_price_median NUMERIC(8,2),
                        cart_adds_same_day_shipping_speed INT,
                        cart_adds_1d_shipping_speed INT,
                        cart_adds_2d_shipping_speed INT,
                        purchases_total_count INT,
                        purchases_purchase_rate NUMERIC(8,2),
                        purchases_asin_count INT,
                        purchases_asin_share NUMERIC(8,2),
                        purchases_price_median NUMERIC(8,2),
                        purchases_asin_price_median NUMERIC(8,2),
                        purchases_same_day_shipping_speed INT,
                        purchases_1d_shipping_speed INT,
                        purchases_2d_shipping_speed INT,
                        reporting_date DATE,
                        created TIMESTAMP WITH TIME ZONE DEFAULT now(),
                        PRIMARY KEY (country, asin, reporting_range, reporting_date, search_query)
);""")


def create_search_query_performance_brand_view(cur):
    cur.execute("""
                    CREATE TABLE search_query_performance_brand_view (
                        country VARCHAR(3) NOT NULL,
                        reporting_range VARCHAR(6) NOT NULL,
                        week INT NOT NULL,
                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,
                        search_query VARCHAR(310),
                        search_query_score INT,
                        search_query_volume INT,
                        impressions_total_count INT,
                        impressions_brand_count INT,
                        impressions_brand_share NUMERIC(8,2),
                        clicks_total_count INT,
                        clicks_click_rate NUMERIC(8,2),
                        clicks_brand_count INT,
                        clicks_brand_share NUMERIC(8,2),
                        clicks_price_median NUMERIC(8,2),
                        clicks_brand_price_median NUMERIC(8,2),
                        clicks_same_day_shipping_speed INT,
                        clicks_1d_shipping_speed INT,
                        clicks_2d_shipping_speed INT,
                        cart_adds_total_count INT,
                        cart_adds_cart_add_rate NUMERIC(8,2),
                        cart_adds_brand_count INT,
                        cart_adds_brand_share NUMERIC(8,2),
                        cart_adds_price_median NUMERIC(8,2),
                        cart_adds_brand_price_median NUMERIC(8,2),
                        cart_adds_same_day_shipping_speed INT,
                        cart_adds_1d_shipping_speed INT,
                        cart_adds_2d_shipping_speed INT,
                        purchases_total_count INT,
                        purchases_purchase_rate NUMERIC(8,2),
                        purchases_brand_count INT,
                        purchases_brand_share NUMERIC(8,2),
                        purchases_price_median NUMERIC(8,2),
                        purchases_brand_price_median NUMERIC(8,2),
                        purchases_same_day_shipping_speed INT,
                        purchases_1d_shipping_speed INT,
                        purchases_2d_shipping_speed INT,
                        reporting_date DATE,
                        created TIMESTAMP WITH TIME ZONE DEFAULT now(),
                        PRIMARY KEY (country, reporting_range, reporting_date, search_query)
);""")


def create_sponsored_products_amazon(cur):
    cur.execute("""
        CREATE TABLE sponsored_products_amazon (
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            portfolio_name VARCHAR(100),
            currency VARCHAR(3) NOT NULL,
            campaign_name VARCHAR(100) NOT NULL,
            ad_group_name VARCHAR(80) NOT NULL,
            targeting VARCHAR(128) NOT NULL,
            match_type VARCHAR(6),
            customer_search_term VARCHAR(249),
            impressions INT,
            clicks SMALLINT,
            ctr NUMERIC(5,2),
            cpc NUMERIC(4,2),
            spend NUMERIC(7,2),
            total_sales NUMERIC(7,2),
            acos NUMERIC(5,2),
            roas NUMERIC(6,2),
            total_orders SMALLINT,
            total_units SMALLINT,
            cvr NUMERIC(3,2),
            advertised_sku_units SMALLINT,
            other_sku_units SMALLINT,
            advertised_sku_sales NUMERIC(6,2),
            other_sku_sales NUMERIC(6,2),
            created TIMESTAMP WITH TIME ZONE DEFAULT now(),
            PRIMARY KEY (start_date, end_date, portfolio_name, currency, campaign_name, ad_group_name, targeting, match_type, customer_search_term)
        );""")

def insert_sponsored_products_amazon(cur):
    cur.execute("""INSERT INTO metadata(table_name, created, column_details) 
                    VALUES ('sponsored_products_amazon', NOW(), 
                            '{"start_date": "Start Date",
                                "end_date": "End Date",
                                "portfolio_name": "Portfolio Name",
                                "currency": "Currency",
                                "campaign_name": "Campaign Name",
                                "ad_group_name": "Ad Group Name",
                                "targeting": "Targeting",
                                "match_type": "Match Type",
                                "customer_search_term": "Customer Search Term",
                                "impressions": "Impressions",
                                "clicks": "Clicks",
                                "ctr": "Click-Thru Rate (CTR)",
                                "cpc": "Cost Per Click (CPC)",
                                "spend": "Spend",
                                "total_sales": "7 Day Total Sales",
                                "acos": "Total Advertising Cost of Sales (ACOS)",
                                "roas": "Total Return on Advertising Spend (ROAS)",
                                "total_orders": "7 Day Total Orders (#)",
                                "total_units": "7 Day Total Units (#)",
                                "cvr": "7 Day Conversion Rate",
                                "advertised_sku_units": "7 Day Advertised SKU Units (#)",
                                "other_sku_units": "7 Day Other SKU Units (#)",
                                "advertised_sku_sales": "7 Day Advertised SKU Sales",
                                "other_sku_sales": "7 Day Other SKU Sales"}');""")

def create_sponsored_display_report(cur):
    cur.execute("""CREATE TABLE Sponsored_Display_Report (
                    record_id INT NOT NULL AUTO_INCREMENT,
                    campaign_id VARCHAR(255),
                    campaign_name VARCHAR(255),
                    ad_group_id VARCHAR(255),
                    ad_group_name VARCHAR(255),
                    targeting_type VARCHAR(255),
                    targeting_expression VARCHAR(255),
                    start_date DATE,
                    end_date DATE,
                    currency VARCHAR(255),
                    impressions INT,
                    clicks INT,
                    click_through_rate DECIMAL(10,2),
                    cost_per_click DECIMAL(10,2),
                    spend DECIMAL(10,2),
                    attributed_conversions_1d INT,
                    attributed_conversions_7d INT,
                    attributed_conversions_14d INT,
                    attributed_conversions_30d INT,
                    attributed_sales_1d DECIMAL(10,2),
                    attributed_sales_7d DECIMAL(10,2),
                    attributed_sales_14d DECIMAL(10,2),
                    attributed_sales_30d DECIMAL(10,2),
                    attributed_units_ordered_1d INT,
                    attributed_units_ordered_7d INT,
                    attributed_units_ordered_14d INT,
                    attributed_units_ordered_30d INT,
                    total_units_ordered_1d INT,
                    total_units_ordered_7d INT,
                    total_units_ordered_14d INT,
                    total_units_ordered_30d INT,
                    conversion_rate_1d DECIMAL(10,2),
                    conversion_rate_7d DECIMAL(10,2),
                    conversion_rate_14d DECIMAL(10,2),
                    conversion_rate_30d DECIMAL(10,2),
                    campaign_status VARCHAR(255),
                    PRIMARY KEY (record_id) );""")


"""
CREATE TABLE sponsored_product_search_term_report (
  date DATE,
  portfolio_name VARCHAR(255),
  currency VARCHAR(3),
  campaign_name VARCHAR(255),
  ad_group_name VARCHAR(255),
  targeting VARCHAR(255),
  match_type VARCHAR(255),
  customer_search_term VARCHAR(255),
  impressions INTEGER,
  clicks INTEGER,
  ctr NUMERIC,
  cpc NUMERIC,
  spend NUMERIC,
  seven_day_total_sales NUMERIC,
  total_advertising_cost_of_sales NUMERIC,
  total_roas NUMERIC,
  seven_day_total_orders INTEGER,
  seven_day_total_units INTEGER,
  seven_day_conversion_rate NUMERIC,
  seven_day_advertised_sku_units INTEGER,
  seven_day_other_sku_units INTEGER,
  seven_day_advertised_sku_sales NUMERIC,
  seven_day_other_sku_sales NUMERIC
);
"""

def create_h10_keyword_tracker(cur):
    cur.execute("""CREATE TABLE h10_keyword_tracker (
                    title VARCHAR(200),
                    asin VARCHAR(15),
                    keyword VARCHAR(100),
                    marketplace VARCHAR(15),
                    search_volume INT,
                    organic_rank INT,
                    sponsored_position INT,
                    date_added TIMESTAMP,
                    PRIMARY KEY(asin, keyword, marketplace, date_added),
                    FOREIGN KEY(asin)
                        REFERENCES product_amazon(asin)
                        ON UPDATE CASCADE
                );""")

def copy_h10_keyword_tracker(cur, path):
    try:
        # cleans data
        data = pd.read_csv(path)
        columns = ['Search Volume', 'Organic Rank', 'Sponsored Position']
        data[columns] = data[columns].replace('-', 0).replace('>306', 306).astype(int)
        data['Date Added'] = pd.to_datetime(data['Date Added'])
        # excludes old data
        query = '''SELECT MAX(date_added) FROM h10_keyword_tracker;'''
        last_date_added = sql_to_dataframe(query)['max'].item()
        if last_date_added:
            data = data[data['Date Added'] > last_date_added]
        # saves
        temp_csv = os.path.join('H10 Keyword Tracker', 'keyword_tracker_temp.csv')
        data.to_csv(temp_csv, index=False, sep='\t')
        with open(temp_csv, 'rb') as file:
            next(file)
            cur.copy_from(file, 'h10_keyword_tracker', sep='\t', null='0', 
                                        columns=(col.replace(' ','_').lower() for col in data.columns))
    except Exception as e:
        print(e)


if __name__ == "__main__":
    cur = setup_cursor()
    # create_h10_keyword_tracker(cur)
    path = os.path.join(os.getcwd(), 'H10 Keyword Tracker', 'helium10-kt-B0B6SYN9NX-2023-05-24.csv')
    copy_h10_keyword_tracker(cur, path)
    # create_search_query_performance_brand_view(cur)
    # create_metadata(cur)
    # create_cerebro_amazon(cur)
    # create_product_amazon(cur)
    # create_search_query_performance_asin_view(cur)
    # create_sponsored_products_amazon(cur)
    # with open('Active Amazon Products.csv', 'r') as f:
    #     next(f) # Skips first line
    #     cur.copy_from(f, 'product_amazon', sep=',')
    pass