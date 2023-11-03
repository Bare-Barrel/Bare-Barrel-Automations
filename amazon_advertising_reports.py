import requests
import json
import pandas as pd
import datetime as dt
import io
import gzip
import time
from ad_api.api.reports import Reports
from ad_api.base.exceptions import AdvertisingApiTooManyRequestsException
from ad_api.base import Marketplaces
from utility import to_list
import postgresql
import re
import os
import ast
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_names = {
        "SPONSORED_PRODUCTS": {
            "spCampaigns": {
                "['campaign']": "campaign",
                "['adGroup']":  "adgroup", #(useless?)
                # "['campaignPlacement']": "placement", # USELESS no pkey (campaignId/adgroupId)
                "['campaign', 'adGroup']": "campaign_adgroup",
                "['campaign', 'campaignPlacement']": "campaign_placement",
                # "['adGroup', 'campaignPlacement']": "adgroup_placement", # can't add adgroup additional metrics (==campaign_placement)
                # "['campaign', 'adGroup', 'campaignPlacement']": "campaign_adgroup_placement", # cannot add adgroup additional metrics (==campaign_placement)
            },
            "spTargeting": {
                "['targeting']": "targeting"
            },
            "spSearchTerm": {
                "['searchTerm']": "search_term"
            },
            "spAdvertisedProduct": {
                "['advertiser']": "advertised_product"
            },
            "spPurchasedProduct": {
                "['asin']": "purchased_product"
            },
            },
        "SPONSORED_BRANDS": {
            "sbPurchasedProduct": {
                "['purchasedAsin']": "purchased_product"
            }
        }
}

# Sponsored ads (version 3) group by metrics
campaign_base_metrics = 'impressions, clicks, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, date, startDate, endDate, campaignBiddingStrategy, costPerClick, clickThroughRate, spend'
campaign_addtl_metrics = 'campaignName, campaignId, campaignStatus, campaignBudgetAmount, campaignBudgetType, campaignRuleBasedBudgetAmount, campaignApplicableBudgetRuleId, campaignApplicableBudgetRuleName, campaignBudgetCurrencyCode, topOfSearchImpressionShare'
adGroup_addtl_metrics = 'adGroupName, adGroupId, adStatus'
campaignPlacement_addtl_metrics = 'placementClassification'
targeting_base_metrics = 'impressions, clicks, costPerClick, clickThroughRate, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, salesOtherSku7d, unitsSoldOtherSku7d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d, keywordId, keyword, campaignBudgetCurrencyCode, date, startDate, endDate, portfolioId, campaignName, campaignId, campaignBudgetType, campaignBudgetAmount, campaignStatus, keywordBid, adGroupName, adGroupId, keywordType, matchType, targeting, topOfSearchImpressionShare'
targeting_addtl_metrics = 'adKeywordStatus'
searchTerm_base_metrics = 'impressions, clicks, costPerClick, clickThroughRate, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, salesOtherSku7d, unitsSoldOtherSku7d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d, keywordId, keyword, campaignBudgetCurrencyCode, date, startDate, endDate, portfolioId, searchTerm, campaignName, campaignId, campaignBudgetType, campaignBudgetAmount, campaignStatus, keywordBid, adGroupName, adGroupId, keywordType, matchType, targeting, adKeywordStatus'
advertiser_base_metrics = 'date, startDate, endDate, campaignName, campaignId, adGroupName, adGroupId, adId, portfolioId, impressions, clicks, costPerClick, clickThroughRate, cost, spend, campaignBudgetCurrencyCode, campaignBudgetAmount, campaignBudgetType, campaignStatus, advertisedAsin, advertisedSku, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, salesOtherSku7d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, unitsSoldOtherSku7d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d'
asin_base_metrics = 'date, startDate, endDate, portfolioId, campaignName, campaignId, adGroupName, adGroupId, keywordId, keyword, keywordType, advertisedAsin, purchasedAsin, advertisedSku, campaignBudgetCurrencyCode, matchType, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, purchases1d, purchases7d, purchases14d, purchases30d, unitsSoldOtherSku1d, unitsSoldOtherSku7d, unitsSoldOtherSku14d, unitsSoldOtherSku30d, salesOtherSku1d, salesOtherSku7d, salesOtherSku14d, salesOtherSku30d, purchasesOtherSku1d, purchasesOtherSku7d, purchasesOtherSku14d, purchasesOtherSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d'
purchasedAsin_base_metrics = 'campaignId, adGroupId, date, startDate, endDate, campaignBudgetCurrencyCode, campaignName, adGroupName, attributionType, purchasedAsin, productName, productCategory, sales14d, orders14d, unitsSold14d, newToBrandSales14d, newToBrandPurchases14d, newToBrandUnitsSold14d, newToBrandSalesPercentage14d, newToBrandPurchasesPercentage14d, newToBrandUnitsSoldPercentage14d'


metrics = {
    # Sponsored Products (version 3)
    'SPONSORED_PRODUCTS': {
        "['campaign']": f'{campaign_addtl_metrics}, {campaign_base_metrics}',
        "['adGroup']": f'{adGroup_addtl_metrics}, {campaign_base_metrics}',
        "['campaign', 'adGroup']": f'{campaign_addtl_metrics}, {adGroup_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
        "['campaignPlacement']": f'{campaignPlacement_addtl_metrics}, {campaign_base_metrics}',     # useless? No campaignIds/adGroupIds
        "['campaign', 'campaignPlacement']": f'{campaign_addtl_metrics}, {campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
        "['adGroup', 'campaignPlacement']": f'{campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '), # can't add adGroup addt'l metrics == campaignPlacement
        "['campaign', 'adGroup', 'campaignPlacement']": f'{campaign_addtl_metrics}, {campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '), # Can't add adGrouop add'tl metrics; == campaign, campaignPlacement
        "['targeting']": f'{targeting_base_metrics}, {targeting_addtl_metrics}',
        "['searchTerm']": searchTerm_base_metrics,
        "['advertiser']": advertiser_base_metrics,
        "['asin']": asin_base_metrics
    },
    # Sponsored Brands Video (version 3)
    'SPONSORED_BRANDS': {
        "['purchasedAsin']": purchasedAsin_base_metrics
    }
}


filters = {
    'spSearchTerm': ['TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED'],
    'spPurchasedProduct': ['BROAD', 'PHRASE', 'EXACT', 'TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED']
}


def request_report(ad_product, report_type_id, group_by, start_date, end_date, time_unit='DAILY', marketplace='US'):
    """
    Requests a Sponsored Products report.
    Request the creation of a performance report for all entities of a single type which have
    performance data to report. Record types can be one of campaigns, adGroups, keywords, 
    productAds, asins, and targets. Note that for asin reports, the report currently can not 
    include metrics associated with both keywords and targets. If the targetingId value is set 
    in the request, the report filters on targets and does not return sales associated with keywords. 
    If the targetingId value is not set in the request, the report filters on keywords and does not 
    return sales associated with targets. Therefore, the default behavior filters the report on keywords. 
    Also note that if both keywordId and targetingId values are passed, the report filters on targets only 
    and does not return keywords.

    Args:
        ad_product (str): ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"]
        report_type_id (str): ['spCampaigns', 'spTargeting', 'spSearchTerm', 'spAdvertisedProduct', 'spPurchasedProduct', 'sbPurchasedProduct']
        group_by (str): will convert into a list spCampaigns: [campaign, adGroup, ad, productAds, placement, category, asin]
        marketplace (str): ['US', 'CA']. It will retrieve the profile_id of the marketplace
    Returns:
        ad_api.api.ApiResponse.payload (dict)
    """
    # Selects date columns as per time unit
    if time_unit == 'DAILY':
        columns = metrics[ad_product][group_by].replace(' startDate, endDate,', '')
    elif time_unit == 'SUMMARY':
        columns = metrics[ad_product][group_by].replace(' date,', '')

    body = {
        "name": f"{ad_product} ({marketplace}) {report_type_id} {group_by} {start_date} - {end_date}",
        "startDate": str(start_date),
        "endDate": str(end_date),
        "configuration":{
            "adProduct": ad_product,
            "groupBy": ast.literal_eval(group_by),    # converts to list
            "columns": columns.split(', '),
            # "filters": [
            #     {
            #         "field": "keywordType",
            #         "values": filters[report_type_id]
            #     }
            # ],
            "reportTypeId": report_type_id,
            "timeUnit": time_unit,
            "format": "GZIP_JSON"
        }
    }
    logger.info(f"Requesting {body['name']}")
    
    sleep_multiplier = 1
    while True:
        try:
            response = Reports(account=marketplace, marketplace=Marketplaces[marketplace]).post_report(body=body)
            payload = response.payload
            return payload
        except AdvertisingApiTooManyRequestsException as e:
            logger.error(e)
            time.sleep(60 * sleep_multiplier)
            sleep_multiplier += 0.05


def download_report(report_id, root_directory, report_name):
    """
    Once you have made a successful POST call, report generation can take up to three hours.
    You can check the report generation status by using the reportId returned in the initial 
    request to call the GET report endpoint: GET /reporting/reports/{reportId}.
    If the report is still generating, status is set to PENDING or PROCESSING.
    When your report is ready to download, status returns as COMPLETED, and you will see an 
    address in the url field.

    Args:
        report_id (str): id of the report to be downloaded after posting a request report
        root_directory (str | os.path): save directory
        report_name (str)

    Returns:
        file_path (str)
    """
    # regrexing save path
    match = re.search(r'(SPONSORED_\w+)\s\((\w*)\)\s(\w+)\s(\[.+\])', report_name)
    ad_product, marketplace, report_type_id, group_by = match[1], match[2], match[3], match[4]
    table_name = table_names[ad_product][report_type_id][group_by]
    directory = os.path.join(root_directory, ad_product, table_name, marketplace)

    if not os.path.exists(directory) and directory:
        logger.info(f"Creating new directory: {directory}")
        os.makedirs(directory)

    while True:
        logger.info(f"Downloading {report_name}")

        try:
            response = Reports(account=marketplace, marketplace=Marketplaces[marketplace]).get_report(reportId=report_id)
            status, url = response.payload['status'], response.payload['url']
            logger.info(f"\tReport status: {status}")

            if status == 'COMPLETED':
                # Download the report
                response = requests.get(url)

                if response.status_code == 200:
                    file_path = os.path.join(directory, report_name + '.json.gz')

                    with open(file_path, 'wb') as file:
                        file.write(response.content)

                    logger.info("File downloaded successfully.")

                    return file_path

                else:
                    logger.info("Failed to download file.")
                    logger.info("\tRedownloading...")

        except Exception as error:
            logger.error(error)

        time.sleep(60)


def request_download_reports(ad_product, report_type_id, group_by, start_date, end_date, directory, time_unit='DAILY', marketplace='US'):
    """Streamlines the process of downloading reports."""
    response = request_report(ad_product, report_type_id, group_by, start_date, end_date, time_unit, marketplace)

    report_id, report_name = response['reportId'], response['name']

    file_path = download_report(report_id, directory, report_name, marketplace)

    return file_path


def combine_data(directory=None, file_paths=[], file_extension='.json.gz'):
    """
    Combines files in a directory and/or in file_paths.
    Inserts `marketplace` and `date` to the returned dataframe.
    """
    # gets all similar file types in a directory
    if directory:
        for dirpath, dirnames, filenames in os.walk(directory):
            filenames = [filename for filename in filenames if file_extension in filename]

        for filename in filenames:
            file_paths.append(os.path.join(dirpath, filename))
    
    # combines data
    combined_data = pd.DataFrame()

    for file_path in file_paths:
        df = pd.read_json(file_path)

        # manually adds marketplace and date
        match = re.search(r'\((\w*)\).+(20\d\d-\d\d-\d\d)', file_path)
        df['marketplace'], df['date'] = match[1], match[2]

        combined_data = pd.concat([df, combined_data], ignore_index=True)

    return combined_data


def update_data(ad_product, report_type_id, start_date, end_date, marketplaces=['US', 'CA', 'UK']):
    """
    Adds upsert data step to request_download_reports by
    combining the downloaded files
    """
    report_ids = {}

    # requests reports
    for group_by in table_names[ad_product][report_type_id]:

        for marketplace in to_list(marketplaces):
            response = request_report(ad_product, report_type_id, group_by, 
                                        start_date, end_date, marketplace=marketplace)
            report_ids[response['name']] = response['reportId']
            time.sleep(10)
    
    # download reports
    gzipped_directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')

    for report_name in report_ids:        
        file_path = download_report(report_ids[report_name], gzipped_directory, report_name)

        # regrexing table name
        match = re.search(r'(SPONSORED_\w+)\s\((\w*)\)\s(\w+)\s(\[.+\])', report_name)
        ad_product, marketplace, report_type_id, group_by = match[1], match[2], match[3], match[4]
        table_name = table_names[ad_product][report_type_id][group_by]
        table_name = f"{ad_product.lower()}.{table_name}"
        # manually ads marketplace
        data = pd.read_json(file_path)
        data['marketplace'] = marketplace

        # upserts data
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def update_all_data(start_date, end_date, marketplaces=['US', 'CA', 'UK']):
    """
    Download all reports & upserts to database.
    It can only update up to 31 days at a time.
    """
    report_ids = {}

    # requests reports
    for ad_product in table_names:

        for report_type_id in table_names[ad_product]:

            for group_by in table_names[ad_product][report_type_id]:

                for marketplace in to_list(marketplaces):
                    response = request_report(ad_product, report_type_id, group_by, 
                                                start_date, end_date, marketplace=marketplace)
                    report_ids[response['name']] = response['reportId']
                    time.sleep(10)
    
    # download reports
    gzipped_directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')

    for report_name in report_ids:        
        file_path = download_report(report_ids[report_name], gzipped_directory, report_name)

        # regrexing table name
        match = re.search(r'(SPONSORED_\w+)\s\((\w*)\)\s(\w+)\s(\[.+\])', report_name)
        ad_product, marketplace, report_type_id, group_by = match[1], match[2], match[3], match[4]
        table_name = table_names[ad_product][report_type_id][group_by]
        table_name = f"{ad_product.lower()}.{table_name}"
        # manually ads marketplace
        data = pd.read_json(file_path)
        data['marketplace'] = marketplace

        # upserts data
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(directory, drop_table_if_exists=False):
    """
    Creates table by iterating all files in a directory & combining data.
    Upserts data by default.
    IMPORTANT!: Overlapping data may result to Primary Key error.
    """

    # regrexing table name
    match = re.search(r'(SPONSORED_\w*)/(.+)', str(directory))
    ad_product, report_type = match[1], match[2]
    table_name = f"{ad_product.lower()}.{table_names[ad_product][report_type]}"

    # combines data in the directory
    combined_data = combine_data(directory, file_extension='.json.gz')

    if combined_data.empty:
        logger.info(f"Table {table_name} is empty.\n\tCancelling table creation & upsertion. . .")
        return

    with postgresql.setup_cursor() as cur:        
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        logger.info(f"Creating table {table_name}")
        postgresql.create_table(cur, file_path=combined_data, file_extension='pandas', table_name=table_name)

        logger.info("\tAdding triggers...")
        postgresql.update_updated_at_trigger(cur, table_name)

        logger.info("\tUpserting data")
        postgresql.upsert_bulk(table_name, combined_data, file_extension='pandas')



if __name__ == '__main__':
    datetime_now = dt.datetime.now()
    day = datetime_now.day
    hour = datetime_now.hour
    # Updates 60 days at the start of the month
    if day == 1 and hour == 1:
        logger.info("-UPDATING SP LAST 60 DAYS-")
        start_date, end_date = dt.date.today() - dt.timedelta(days=60), dt.date.today()
        update_all_data(start_date, end_date)
    # Updates last 2 weeks at the middle of the month
    elif day == 15 and hour == 1:
        logger.info("-UPDATING SP LAST 15 DAYS-")
        start_date, end_date = dt.date.today() - dt.timedelta(days=15), dt.date.today()
        update_all_data(start_date, end_date)
    # Updates last 7 days daily
    elif day not in (1, 15) and hour == 1:
        logger.info("-UPDATING SP LAST 7 DAYS-")
        start_date, end_date = dt.date.today() - dt.timedelta(days=7), dt.date.today()
        update_all_data(start_date, end_date)
    # Updates `targeting` every hour
    elif day not in (1, 15) and hour != 1:
        logger.info("-UPDATING SP Targeting LAST 2 DAYS-")
        start_date, end_date = dt.date.today() - dt.timedelta(days=1), dt.date.today()
        update_data('SPONSORED_PRODUCTS', 'spTargeting', start_date, end_date)
