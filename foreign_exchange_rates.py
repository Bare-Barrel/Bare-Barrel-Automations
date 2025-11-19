import requests
import pandas as pd
import datetime as dt
import pandas_gbq
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
                    "date": pd.to_datetime(date_str),
                    "source": source,
                    "target": target[-3:],
                    "rate": rate,
                }
            )

    df = pd.DataFrame(rows)
    df["rate"] = df["rate"].apply(lambda x: Decimal(str(x)))
    df = df.sort_values(by='date')

    logger.info("Done converting API response to pandas DataFrame.")

    return df


def load_to_bigquery(df, table_id, project_id):
    """
    Load dataframe to a BigQuery table.
    """
    try:
        logger.info("Loading DataFrame to BigQuery...")
        pandas_gbq.to_gbq(
            df, destination_table=table_id, project_id=project_id, if_exists='append'
        )
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.info("Error loading data to BigQuery:", e)


if __name__ == '__main__':
    """
    currency_from should be a currency code string, e.g., "USD".
    currency_to should be currency code/s in string, comma-delimited, e.g., "CAD,GBP"
    """
    currency_from = "USD"
    currency_to = "CAD,GBP"
    # start_date = dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc).strftime("%Y-%m-%d")
    start_date = dt.date.today().strftime("%Y-%m-%d")
    end_date = dt.date.today().strftime("%Y-%m-%d")

    payload = fetch_rates(currency_from, currency_to, start_date, end_date)

    df = payload_to_dataframe(payload)

    table_id = "modern-sublime-383117.exchangerate_host.exchange_rates"
    project_id = "modern-sublime-383117"

    if not df.empty:
        load_to_bigquery(df, table_id=table_id, project_id=project_id)
