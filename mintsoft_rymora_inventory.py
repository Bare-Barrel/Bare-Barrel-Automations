import requests
import pandas as pd
import pandas_gbq
from google.auth import default as google_auth_default
import json
import logging
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open("config.json") as f:
    config = json.load(f)


def generate_api_key(username: str, password: str) -> str:
    """
    Generate a Mintsoft API key using username/password.

    Args:
        username: Your Mintsoft username.
        password: Your Mintsoft login password (set via Settings → Change Password).
    Returns:
        The API key string from Mintsoft.
    """
    url = "https://api.mintsoft.co.uk/api/Auth"  # Auth endpoint

    # JSON payload required by the API
    payload = {
        "Username": username,
        "Password": password
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to get API key: {e}")
    

def fetch_warehouses(api_key: str):
    """
    Fetches a list of warehouses from Mintsoft.

    Args:
        api_key: A valid Mintsoft API key (ms-apikey header).
    """
    url = "https://api.mintsoft.co.uk/api/Warehouse"
    headers = {
        "ms-apikey": api_key,
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError on 4xx/5xx
    except requests.RequestException as e:
        raise RuntimeError(f"Error getting warehouse info: {e}")
    
    return response.json()
    

def fetch_stock_levels(api_key: str, account_name: str, warehouse_name: str, warehouse_id: int = None, breakdown: bool = False):
    """
    Get inventory stock levels from the Mintsoft API.

    Args:
        api_key: API key returned from generate_api_key()
        warehouse_id: (optional) filter by a specific warehouse ID
        breakdown: include detailed breakdown (if supported)
    """
    base_url = "https://api.mintsoft.co.uk/api/Product/StockLevels"

    # Build query params
    params = {}
    if warehouse_id is not None:
        params["WarehouseId"] = warehouse_id
    if breakdown:
        params["Breakdown"] = "true"  # if supported

    headers = {
        "ms-apikey": api_key,
        "accept": "application/json"
    }

    try:
        logger.info(f"Fetching {account_name} {warehouse_name} warehouse stock level data from Mintsoft API...")
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        logger.info(f"Done fetching {account_name} {warehouse_name} warehouse stock level data from Mintsoft API.")

        # Parse and return JSON response
        return response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Error getting stock levels: {e}")


def json_to_dataframe(json_response, account_name, warehouse_name):
    """
    Convert the API JSON into a pandas DataFrame suitable for BigQuery.

    Args:
        json_response: json response from the API.
        account_name: Mintsoft account name.
        warehouse_name: warehouse name.
    """
    logger.info("Converting API response to pandas DataFrame...")
    df = pd.json_normalize(json_response)

    # Add fields
    df["warehouse_name"] = warehouse_name
    df["account_name"] = account_name
    recorded_at = pd.Timestamp.utcnow()
    df["recorded_at"] = recorded_at

    # Sort by SKU
    df = df.sort_values(
        by=["SKU"],
        ascending=[True]
    )

    logger.info("Done converting API response to pandas DataFrame.")

    return df


def load_to_bigquery(df, table_id: str, project_id: str = "modern-sublime-383117"):
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


def update_data():
    """
    Updates inventory data from Mintsoft.
    TSP API Key renews daily while others never expire.
    """
    tsp_user = config["mintsoft_tsp_username"]
    tsp_pwd = config["mintsoft_tsp_password"]

    # Get API key for TSP
    tsp_api_key = generate_api_key(tsp_user, tsp_pwd)

    accounts = [
        {
            "account_name": "ALLEGRO MAIN",
            "api_key": config["mintsoft_allegro_main_api_key"],
            "warehouses": [
                {"id": "3", "name": "NAILSEA"},
                {"id": "8", "name": "CLEVEDON"},
            ],
        },
        {
            "account_name": "ALLEGRO REWORK",
            "api_key": config["mintsoft_allegro_rework_api_key"],
            "warehouses": [
                {"id": "3", "name": "NAILSEA"},
                {"id": "8", "name": "CLEVEDON"},
            ],
        },
        {
            "account_name": "TSP",
            "api_key": tsp_api_key,
            "warehouses": [
                {"id": "9", "name": "YELLOW"},
            ],
        },
    ]

    dfs = []

    # Fetch data
    for account in accounts:
        account_name = account["account_name"]
        api_key = account["api_key"]

        for warehouse in account["warehouses"]:
            warehouse_id = warehouse["id"]
            warehouse_name = warehouse["name"]
            
            json_response = fetch_stock_levels(api_key, account_name, warehouse_name, warehouse_id)
            df = json_to_dataframe(json_response, account_name, warehouse_name)
            dfs.append(df)
    
    # Combine dataframes
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv("output.csv", index=False, encoding="utf-8")

    # Load data to BigQuery
    table_id = "modern-sublime-383117.mintsoft_api.warehouse_stock_levels"
    load_to_bigquery(final_df, table_id)


if __name__ == "__main__":
    update_data()

    # Get warehouse info
    # warehouses = fetch_warehouses(api_key)
    # df = pd.DataFrame(warehouses)
    # print("Warehouses:")
    # df.to_csv("output.csv", index=False, encoding="utf-8")
    # print(df)