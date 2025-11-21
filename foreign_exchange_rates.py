import requests
import pandas as pd
from datetime import date, timedelta
import pandas_gbq
from google.cloud import bigquery
from google.auth import default as google_auth_default
from decimal import Decimal
import json
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)


with open("config.json") as f:
    config = json.load(f)


def fetch_rates(currency_from, currency_to, start_date, end_date):
    """
    Fetch FX data from exchangerate.host
    """
    url = "https://api.exchangerate.host/timeframe"
    access_key = config['exchangerate_host_access_key']

    params = {
        "access_key": access_key,
        "base": currency_from,
        "currencies": currency_to,
        "start_date": start_date,
        "end_date": end_date,
    }

    try:
        logger.info("Fetching foreign exchange rate data from exchangerate.host...")
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        logger.info("Done fetching foreign exchange rate data from exchangerate.host.")
        return resp.json()
    except requests.exceptions.RequestException as e:
        # Catches network issues, timeouts, DNS errors, etc.
        logger.info("HTTP/network error: ", e)
    except ValueError as e:
        # Catches API-reported errors via the JSON
        logger.info("API error: ", e)


def payload_to_dataframe(payload):
    """
    Convert the API JSON into a pandas DataFrame suitable for BigQuery.
    """
    logger.info("Converting API response to pandas DataFrame...")
    source = payload.get("source", "USD")
    rates = payload.get("quotes", {})

    rows = []
    for date_str, rate_dict in rates.items():
        for target, rate in rate_dict.items():
            rows.append(
                {
                    "recorded_at": pd.to_datetime(date_str),
                    "base": source,
                    "target": target[-3:],
                    "rate": rate,
                }
            )

    df = pd.DataFrame(rows)
    df["rate"] = df["rate"].apply(lambda x: Decimal(str(x)))
    df = df.sort_values(by='recorded_at')

    logger.info("Done converting API response to pandas DataFrame.")

    return df


def load_to_bigquery(df, table_id, project_id):
    """
    Load dataframe to a BigQuery table.
    """
    try:
        logger.info("Loading DataFrame to BigQuery...")
        credentials, project = google_auth_default()
        pandas_gbq.to_gbq(
            df, 
            destination_table=table_id, 
            project_id=project_id, 
            credentials=credentials,    # automatically loaded from env
            if_exists='append'
        )
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.info("Error loading data to BigQuery:", e)


def remove_duplicates(project_id):
    """
    Recreates table after removing duplicates relative to recorded_at, source, and target columns and orders by recorded_at.
    """

    try:
        logger.info("Removing duplicates in BigQuery table...")
        client = bigquery.Client(project=project_id)

        sql = """
        CREATE OR REPLACE TABLE exchangerate_host.exchange_rates AS
        SELECT recorded_at, base, target, rate
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY recorded_at, base, target ORDER BY recorded_at ASC) AS rn_num
            FROM exchangerate_host.exchange_rates
        )
        WHERE rn_num = 1
        ORDER BY recorded_at ASC, target ASC;
        """

        query_job = client.query(sql)
        query_job.result()  # wait for completion
        logger.info("Done removing duplicates.")
    except Exception as e:
        logger.info("Error removing duplicates on BigQuery table:", e)


if __name__ == '__main__':
    """
    currency_from should be a currency code string, e.g., "USD".
    currency_to should be currency code/s in string, comma-delimited, e.g., "CAD,GBP"
    By default, it updates data for yesterday and today.
    """
    currency_from = "USD"
    currency_to = "CAD,GBP"
    # start_date = dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc).strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = date.today().strftime("%Y-%m-%d")
    table_id = "modern-sublime-383117.exchangerate_host.exchange_rates"
    project_id = "modern-sublime-383117"

    payload = fetch_rates(currency_from, currency_to, start_date, end_date)

    df = payload_to_dataframe(payload)

    if not df.empty:
        load_to_bigquery(df, table_id=table_id, project_id=project_id)
    
    remove_duplicates(project_id=project_id)
