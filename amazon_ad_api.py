import logging
from ad_api.base import AdvertisingApiException
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces
import json

amazon_ads_api_config_path = "amazon_ads_api_env.json"
with open(amazon_ads_api_config_path) as f:
    config = json.load(f)

credentials = dict(
    refresh_token=config['refresh_token'],
    client_id=config['client_id'],
    client_secret=config['client_secret'],
    profile_id=config['profile_id']['us'],
)

try:
    status = 'enabled'
    result=sponsored_products.Campaigns(marketplace=Marketplaces.US, credentials=credentials, debug=True).list_campaigns(
        stateFilter=status
    )
    payload = result.payload
    logging.info(payload)
except AdvertisingApiException as error:
    logging.info(error)


from ad_api.api.reports import Reports

body = {
    "startDate": "2022-11-01",
    "endDate": "2022-11-01",
     "configuration": {
        "adProduct": "SPONSORED_PRODUCTS",
        "groupBy": ["advertiser"],
        "columns": ["impressions", "clicks", "cost", "campaignStatus", "advertisedAsin", "date"],
        "reportTypeId": "spAdvertisedProduct",
        "timeUnit": "DAILY",
        "format": "GZIP_JSON"
    }
}

result = Reports(marketplace=Marketplaces.US, credentials=credentials).post_report(body=json.dumps(body))
payload = result.payload
report_id = payload.get('reportId')