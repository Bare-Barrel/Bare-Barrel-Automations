import psycopg2
import psycopg2.extras
import json
import os
import pandas as pd
import numpy as np
import datetime as dt
import re


with open("config.json") as f:
    config = json.load(f)



def setup_cursor(autocommit=True, cursor_factory=psycopg2.extras.RealDictCursor):
    """
    Sets up psycopg2 cursor with the default configuration. The configuration file is
    found on the residing directory. 
    
    Args: 
        autocommit (bool): Omits the conn.commit()
        cursor_factory (psycopg2.extras.*): By default, it's psycopg2.extras.NamedTupleCursor
    Returns:
        conn.cursor()
    """
    # Establishing connection to database (Make sure to sudo service postgresql start)
    conn = psycopg2.connect(f"dbname={config['postgres_db']} user={config['postgres_user']} host={config['postgres_host']} port={config['postgres_port']} password={config['postgres_password']}")
    conn.set_session(autocommit = autocommit)
    cur = conn.cursor(cursor_factory = cursor_factory)      # https://www.psycopg.org/docs/extras.html#real-dictionary-cursor
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


def sql_standardize(name, remove_parenthesis=True, remove_file_extension=True):
    """Standardizes column & table names according to SQL naming convention.
    Column & table names that starts with a numeric character will always
    enclose it with quotes. For example, 14_day_sales would be "14_day_sales".
    Explanation for the parser limitation here: https://stackoverflow.com/questions/15917064/table-or-column-name-cannot-start-with-numeric

    Args:
        name (str): column / table name
        remove_parenthesis (bool): removes parenthesis and inside of it

    Returns: 
        name (str): cleaned name
    """
    # Remove file extension
    if remove_file_extension:
        name = re.sub(r'\..+', '', name)
    # Remove parenthesis and inside of it
    if remove_parenthesis:
        name = re.sub(r'\(.+\)', '', name)
    # Convert to lowercase
    name = name.lower()
    # Replace special characters with underscores
    name = re.sub(r'\W+', '_', name)
    # Remove leading and trailing underscores
    name = name.strip('_')
    # Replace consecutive underscores with a single underscore
    name = re.sub(r'_+', '_', name)
    # Encloses with parenthesis
    if name[0].isdigit():
        name = f'"{name}"'
    return name
    

def create_table(cur, file_path, table_name='filename', created_at=True, updated_at=True, keys=None):
    """Reads an excel or csv file, then normalizes table columns with generic data types"""
    # Read file
    pd_read_file = {'.csv': pd.read_csv, '.xls': pd.read_excel, '.xlsx': pd.read_excel}
    file_extension = os.path.splitext(file_path)[-1]
    data = pd_read_file[file_extension](file_path)

    # Create the SQL CREATE TABLE statement
    if table_name == 'filename':
        base_filename = os.path.basename(file_path)
        table_name = sql_standardize(base_filename)
    create_table_sql = f"CREATE TABLE {table_name} ("

    # Iterate through each column, cleans & identifies data type
    for column_name in data.columns:
        standardized_column_name = sql_standardize(column_name)
        data_type = str(data[column_name].dtype)
        # Map pandas data types to generic PostgreSQL data types
        if 'int' in data_type:
            data_type = 'integer'
        elif 'float' in data_type:
            data_type = 'numeric'
        elif 'datetime' in data_type:
            # identifies if datetime col has time
            has_time = ~(data[column_name].dt.time == pd.to_datetime('00:00:00').time()).all()
            data_type = 'date'
            if has_time:
                data_type = 'timestamp'
        elif 'bool' in data_type:
            data_type = 'boolean'                
        else:
            data_type = 'text'
    
        # Add the column and its data type to the CREATE TABLE statement
        create_table_sql += f"{standardized_column_name} {data_type}, "

    # Add created_at and updated_at to query
    if created_at:
        create_table_sql += "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
    if updated_at:
        create_table_sql += "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "

    # Remove trailing comma and space and enclose it
    create_table_sql = create_table_sql[:-2]
    create_table_sql += ")"

    # Add primary and foreign key constraints
    if keys:
        created_table_sql += '\n' + keys

    # Complete the CREATE TABLE statement
    create_table_sql += ";"
    print(create_table_sql)
    # Execute the CREATE TABLE statement
    cur.execute(create_table_sql)


def create_updated_at_triggers(cur):
    "Create a trigger function to update the 'updated_at' column"
    cur.execute("""CREATE OR REPLACE FUNCTION update_updated_at()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;""")


def update_updated_at_trigger(cur, table_names=[]):
    """The CREATE TRIGGER statement creates a trigger named 
    update_updated_at_trigger that fires before an update operation on 
    the table. It executes the update_updated_at() trigger function for 
    each row being updated, which sets the updated_at column to the current date and time."""
    if isinstance(table_names, str):
        table_names = [table_names]
    table_names = " ".join(table_names)
    cur.execute(f"""CREATE TRIGGER update_updated_at_trigger
                    BEFORE UPDATE ON {table_names}
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at();""")


def create_sponsored_data(cur, directory):
    """Create sponsored tables by specifying a directory"""
    directory = 'PPC Data'
    for file in os.listdir(directory):
        table_name = sql_standardize(file)
        print(table_name)

    

def upsert_bulk(table_name, file_path, temp_path='temp.csv') -> None:
    """
    Fast way to upsert multiple entries at once

    table_name (str):
    file_path (str|os.path): path to csv / excel
    temp_path (str|os.path): save location for the temp csv file
    """
    conn = psycopg2.connect(f"dbname={config['postgres_db']} user={config['postgres_user']} host={config['postgres_host']} port={config['postgres_port']} password={config['postgres_password']}")
    cur = conn.cursor()
    # Extracts table's schema (column names & data type)
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
    schema = cur.fetchall()

    # SQL standardize and pop out `created_at` & `updated_at`
    schema = [(sql_standardize(item[0]), item[-1]) for item in schema if item[0] not in ('created_at', 'updated_at')]

    # Mapping PostgreSQL data types to Python/pandas data types
    type_mapping = {
        'integer': int,
        'bigint': int,
        'smallint': int,
        'numeric': float,
        'real': float,
        'double precision': float,
        'boolean': bool,
        'date': 'datetime64[ns]',  # You can convert it to a proper Python date type if needed
        'timestamp': 'datetime64[ns]',  # You can convert it to a proper Python datetime type if needed
        'timestamp without time zone': 'datetime64[ns]',
        'character varying': str
    }

    # Convert schema data types to Python data types
    converted_schema = []
    for column_name, data_type in schema:
        python_data_type = type_mapping.get(data_type, str) # str if not exists
        converted_schema.append((column_name, python_data_type))

    # Create string with set of columns to be updated
    update_set = ", ".join([f"{column[0]}=EXCLUDED.{column[0]}" for column in converted_schema])

    # Creates temporary empty table with same columns and types as
    # the final table
    temp_table_name = f"temp_{table_name}"
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {temp_table_name} (LIKE {table_name})
        ON COMMIT DROP;
        ALTER TABLE {temp_table_name}
            DROP COLUMN IF EXISTS created_at,
            DROP COLUMN IF EXISTS updated_at;
        """
    )
    
    # Reads file
    pd_read_file = {'.csv': pd.read_csv, '.xls': pd.read_excel, '.xlsx': pd.read_excel}
    file_extension = os.path.splitext(file_path)[-1]
    data = pd_read_file[file_extension](file_path)
    # standardize column names
    data.columns = [sql_standardize(column) for column in data.columns]
    print(data.columns)
 
    # Casts appropriate data type
    for column in converted_schema:
        column_name, data_type = column[0], column[1]
        # percentage str columns
        if data_type in (int, float) and data[column_name].dtype == 'object' and data[column_name].str.contains('%').any():
            data[column_name] = data[column_name].str.replace('%', '').astype(float) / 100
        data[column_name] = data[column_name].astype(data_type)

    # transforms csv/excel to tab delimited file
    data.to_csv(temp_path, sep=',', index=False)

    # Copy stream data to the created temporary table in DB
    with open(temp_path, 'rb') as file:
        next(file)
        # cur.copy_from(file, temp_table_name, null='', sep=',')
        cur.copy_expert(f"COPY {temp_table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"')", file)

    # Execute a query to get the primary key constraint name
    cur.execute("""SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = (
                        SELECT oid
                        FROM pg_class
                        WHERE relname = %s
                    ) AND contype = 'p';""", (table_name,))
    primary_key_constraint_name = cur.fetchone()[0]

    # Inserts copied data from the temporary table to the final table
    # updating existing values at each new conflict
    cur.execute(
        f"""
        INSERT INTO {table_name}({', '.join(data.columns)})
        SELECT * FROM {temp_table_name}
        ON CONFLICT ON CONSTRAINT sponsored_products_search_term_report_pkey DO UPDATE SET {update_set}
        """
    )

    # Drops temporary table (I believe this step is unnecessary,
    # but tables sizes where growing without any new data modifications
    # if this command isn't executed)
    cur.execute(f"DROP TABLE {temp_table_name}")

    # Commit everything through cursor
    conn.commit()
    conn.close()


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
            sku VARCHAR(50) NOT NULL
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
    jls_extract_var = """INSERT INTO metadata(table_name, created, column_details) 
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
                                    "other_sku_sales": "7 Day Other SKU Sales"}');"""
    cur.execute(jls_extract_var)


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


def create_sponsored_product_search_term_report (cur):
    cur.execute("""CREATE TABLE sponsored_product_search_term_report (
                    date DATE,
                    portfolio_name VARCHAR(255),
                    currency VARCHAR(3),
                    campaign_name VARCHAR(255),
                    ad_group_name VARCHAR(255),
                    targeting VARCHAR(255),
                    match_type VARCHAR(255),
                    cutomer_search_term VARCHAR(255),
                    impressions INTEGER,
                    clicks INTEGER,
                    ctr NUMERIC,
                    cpc NUMERIC,
                    spend NUMERIC,
                    7_day_total_sales NUMERIC,
                    total_advertising_cost_of_sales NUMERIC,
                    total_roas NUMERIC,
                    7_day_total_orders INTEGER,
                    7_day_total_units INTEGER,
                    7_day_conversion_rate NUMERIC,
                    7_day_advertised_sku_units INTEGER,
                    7_day_other_sku_units INTEGER,
                    7_day_advertised_sku_sales NUMERIC,
                    7_day_other_sku_sales NUMERIC
                    );""")


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
    # path = os.path.join(os.getcwd(), 'H10 Keyword Tracker', 'helium10-kt-B0B6SYN9NX-2023-05-24.csv')
    # copy_h10_keyword_tracker(cur, path)
    # create_search_query_performance_brand_view(cur)
    # create_metadata(cur)
    # create_cerebro_amazon(cur)
    # create_product_amazon(cur)
    # create_search_query_performance_asin_view(cur)
    # create_sponsored_products_amazon(cur)
    # with open('Active Amazon Products.csv', 'r') as f:
    #     next(f) # Skips first line
    #     cur.copy_from(f, 'product_amazon', sep=',')
    file
    csv = os.path.join(os.getcwd(), 'PPC Data', 'ppc_temp.csv')
    upsert_bulk('sponsored_product_search_term_report', file_path=csv)
    pass