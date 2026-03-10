import requests
import pandas as pd
from google.auth import default as google_auth_default
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
import bigquery_utils
import json
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open("config.json") as f:
    config = json.load(f)


PROJECT_ID = "modern-sublime-383117"
SOURCE_DATASET = "dbt_prod_marts"
SOURCE_TABLE = "obt_mintsoft_warehouse_stock_levels"
DEST_DATASET = "mintsoft_api"
DEST_TABLE = "allegro_product_inventory"

ACCOUNTS = [
        {
            "account_name": "ALLEGRO MAIN",
            "api_key": config["mintsoft_allegro_main_api_key"],
        },
        {
            "account_name": "ALLEGRO REWORK",
            "api_key": config["mintsoft_allegro_rework_api_key"],
        }
    ]


def get_product_ids(account):
    """
    Get product_id's from the existing Mintsoft inventory table.

    Args:
        account: Allegro account (ALLEGRO MAIN or ALLEGRO REWORK)
    """

    try:
        logger.info(f"Fetching Product IDs for {account['account_name']}...")
        
        client = bigquery.Client(project=PROJECT_ID)

        sql = f"""
            SELECT DISTINCT product_id
            FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
            WHERE product_id IS NOT NULL AND account_name = "{account["account_name"]}"
            """

        query_job = client.query(sql)
        query_job.result()  # wait for completion
        df = query_job.to_dataframe()

        logger.info(f"Done fetching Product IDs for {account['account_name']}.")

        return df["product_id"].tolist()

    except Exception as e:
        logger.info("Error getting Product IDs: ", e)



def fetch_all_inbound_inventory(account, product_ids):
    """
    Get all inbound inventory from the Mintsoft API.

    Args:
        account: Allegro account (ALLEGRO MAIN or ALLEGRO REWORK)
        product_id: product_id's fetch from existing mintsoft inventory table
    """

    MAX_WORKERS = 10
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(fetch_inbound_inventory, account, pid)
            for pid in product_ids
        ]

        for future in as_completed(futures):

            result = future.result()

            if result:
                results.append(result)

    return results


def fetch_inbound_inventory(account, product_id):
    """
    Get inbound inventory from the Mintsoft API.

    Args:
        account: Allegro account (ALLEGRO MAIN or ALLEGRO REWORK)
        product_id: product_id's fetch from existing mintsoft inventory table
    """
    BASE_URL = "https://api.mintsoft.co.uk/api/Product"
    url = f"{BASE_URL}/{int(product_id)}/Inventory"

    headers = {
        "ms-apikey": account["api_key"],
        "accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        if response.status_code == 200:
            data = response.json()

            results = []

            for row in data:
                row["account_name"] = account["account_name"]
                results.append(row)

            return results

        return None

    except Exception as e:
        logger.info(f"Request failed for account {account["account_name"]} and Product ID {product_id}: {e}")
        return None


def update_data():
    """
    Updates inbound inventory data from Mintsoft for Allegro MAIN and REWORK.
    """
    all_results = []

    for account in ACCOUNTS:

        # Get Product IDs
        product_ids = get_product_ids(account)
        
        logger.info(f"Fetching inbound inventory for {account['account_name']}...")

        results = fetch_all_inbound_inventory(account, product_ids)

        all_results.extend(results)

        logger.info(f"Done fetching inbound inventory for {account['account_name']}.")

    flattened_results = [item for sublist in all_results for item in sublist]
    df = pd.DataFrame(flattened_results)
    df["recorded_at"] = pd.Timestamp.utcnow()

    # Sort by SKU
    df = df.sort_values(
        by = ["account_name", "SKU"],
        ascending = [True, True]
    )

    df.to_csv("output.csv", index=False, encoding="utf-8")

    # Load data to BigQuery
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(df, table_id, PROJECT_ID, "append")


if __name__ == "__main__":
    update_data()