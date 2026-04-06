from sp_api.api import ProductFees
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiRequestThrottledException, SellingApiServerException
import time
import pandas as pd
import logging
import logger_setup
from google.cloud import bigquery
import bigquery_utils

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_name = 'business_reports.fba_fee_preview'
tenants = bigquery_utils.get_tenants()

PROJECT_ID = "modern-sublime-383117"
SOURCE_DATASET = "listings_items"
SOURCE_TABLE = "offers"
DEST_DATASET = "product_fees"
DEST_TABLE = "product_fees_estimates"


def get_sku_list():
    """
    Get SKUs with corresponding price and currency codes from the listings_items table.
    """
    try:
        logger.info("Getting SKU list from BigQuery listings_items.offers table...")
        client = bigquery.Client(project=PROJECT_ID)

        sql = """
        SELECT date, sku, tenant_id, marketplace, price_amount, price_currency_code
        FROM `listings_items.offers`
        WHERE date = (SELECT MAX(date) FROM `listings_items.offers`)
            AND price_amount IS NOT NULL
            AND price_amount <> 0
            AND REGEXP_CONTAINS(sku, "^BB_|^R_");
        """

        query_job = client.query(sql)
        df = query_job.to_dataframe()

        logger.info("Done getting SKU list from BigQuery listings_items.offers table.")

        # df.to_csv("output.csv", index=False, encoding="utf-8")

        return df
    except Exception as e:
        logger.info("Error removing duplicates on BigQuery table:", e)


def chunked(df, size = 20):
    """
    Helper function
    Group in chunks of 20
    """
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]


def build_requests(batch_df, account, marketplace):
    """
    Helper function
    Build requests json for input to get_product_fees_estimate()
    """
    marketplace_id = Marketplaces[marketplace].marketplace_id

    requests = []
    for _, row in batch_df.iterrows():
        requests.append({
            "id_type": "SellerSKU",
            "id_value": row["sku"],
            "price": float(row["price_amount"]),
            "currency": row["price_currency_code"],
            "shipping_price": 0,
            "is_fba": True,
            "marketplace_id": marketplace_id
        })
    return requests


def fetch_fba_fees(df):
    """
    Get estimated FBA fees for each SKU. Processes in batches of 20 SKUs.

    Args:
        df: Dataframe of SKUs with corresponding prices and currency codes
    """
    results = []

    # Group by tenant + marketplace
    grouped = df.groupby(["tenant_id", "marketplace"])

    reverse_tenants = {v: k for k, v in tenants.items()}

    for (tenant_id, marketplace), group_df in grouped:

        logger.info(f"Processing tenant={tenant_id} and marketplace={marketplace}...")

        account = reverse_tenants.get(tenant_id)
        for batch_df in chunked(group_df, 20):
            requests = build_requests(batch_df, account, marketplace)

            try:
                response = ProductFees(
                        account=f'{account}-{marketplace}',
                        marketplace=Marketplaces[marketplace]
                    ).get_product_fees_estimate(requests)

                for res in response.payload:
                    if res.get("Status") != "Success":
                        error = res.get("Error", {})
                        logger.warning(
                            f"Fee estimate failed for SKU: "
                            f"{res.get('FeesEstimateIdentifier', {}).get('IdValue')} | "
                            f"{error.get('Code')} - {error.get('Message')}"
                        )
                        results.append({
                            "tenant_id": tenant_id,
                            "marketplace": marketplace,
                            "sku": res.get("FeesEstimateIdentifier", {}).get("IdValue"),
                            "price_to_estimate_fees_amount": res.get("FeesEstimateIdentifier", {}).get("PriceToEstimateFees", {}).get("ListingPrice", {}).get("Amount"),
                            "price_to_estimate_fees_currency_code": res.get("FeesEstimateIdentifier", {}).get("PriceToEstimateFees", {}).get("ListingPrice", {}).get("CurrencyCode"),
                            "fees_estimated_at": None,
                            "est_total_fees": None,
                            "est_referral_fee": None,
                            "est_variable_closing_fee": None,
                            "est_per_item_fee": None,
                            "est_fba_fee": None,
                            "error_code": res.get("Error", {}).get("Code"),
                            "error_message": res.get("Error", {}).get("Message")
                        })
                        continue  # store failures in output for visibility
                    
                    identifier = res.get("FeesEstimateIdentifier", {})
                    sku = identifier.get("IdValue")

                    fees_estimated_at = res.get("FeesEstimate", {}).get("TimeOfFeesEstimation")
                    total_fees = res.get("FeesEstimate", {}).get("TotalFeesEstimate", {}).get("Amount")

                    # Get fees
                    fees = res.get("FeesEstimate", {}).get("FeeDetailList", [])
                    referral_fee = None
                    variable_closing_fee = None
                    per_item_fee = None
                    fba_fee = None
                    for fee in fees:
                        if fee.get("FeeType") == "ReferralFee":
                            referral_fee = fee.get("FinalFee", {}).get("Amount")
                        if fee.get("FeeType") == "VariableClosingFee":
                            variable_closing_fee = fee.get("FinalFee", {}).get("Amount")
                        if fee.get("FeeType") == "PerItemFee":
                            per_item_fee = fee.get("FinalFee", {}).get("Amount")
                        if fee.get("FeeType") == "FBAFees":
                            fba_fee = fee.get("FinalFee", {}).get("Amount")

                    results.append({
                        "tenant_id": tenant_id,
                        "marketplace": marketplace,
                        "sku": sku,
                        "price_to_estimate_fees_amount": res.get("FeesEstimateIdentifier", {}).get("PriceToEstimateFees", {}).get("ListingPrice", {}).get("Amount"),
                        "price_to_estimate_fees_currency_code": res.get("FeesEstimateIdentifier", {}).get("PriceToEstimateFees", {}).get("ListingPrice", {}).get("CurrencyCode"),
                        "fees_estimated_at": fees_estimated_at,
                        "est_total_fees": total_fees,
                        "est_referral_fee": referral_fee,
                        "est_variable_closing_fee": variable_closing_fee,
                        "est_per_item_fee": per_item_fee,
                        "est_fba_fee": fba_fee
                    })

                    logger.info(f"Done getting estimated fees for SKU: {sku}")
            except (SellingApiRequestThrottledException, SellingApiServerException) as error:
                logger.warning(error)

            # Rate limit protection (~0.5 RPS)
            time.sleep(2)

    results_df = pd.DataFrame(results)
    results_df["recorded_at"] = pd.Timestamp.now(tz="UTC")

    return results_df


def update_data():
    sku_df = get_sku_list()

    fba_fees_df = fetch_fba_fees(sku_df)
    
    # fba_fees_df.to_csv("output.csv", index=False, encoding="utf-8")

    # Load data to BigQuery
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    bigquery_utils.load_to_bigquery(fba_fees_df, table_id, PROJECT_ID, "append")


if __name__ == '__main__':
    update_data()