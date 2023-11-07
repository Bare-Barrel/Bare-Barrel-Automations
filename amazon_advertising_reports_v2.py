from ad_api.api.sb.reports import Reports as sb_Reports
from ad_api.api.sd.reports import Reports as sd_Reports
from ad_api.base import Marketplaces
from ad_api.base.exceptions import AdvertisingApiTooManyRequestsException
from utility import to_list
import datetime as dt
import pandas as pd
import json
import time
import os
import re
import io
import csv
import requests
from requests.exceptions import ConnectionError
import postgresql
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

table_names = {
        'SPONSORED_BRANDS': {
            'campaigns': 'campaign_v2',
            'campaigns - placement': 'campaign_placement_v2',
            'adGroups': 'adgroup_v2',
            'targets': 'targeting_v2',
            'keywords': 'keyword_v2',
            'keywords - query': 'search_term_v2',
            'ads': 'ad_v2',
        },

        'SPONSORED_DISPLAY': {
            'campaigns': 'campaign_v2',
            'adGroups': 'adgroup_v2',
            'productAds': 'advertised_product_v2',
            'targets': 'targeting_v2',
            # 'asins': 'purchased_product', # broken
            'campaigns - matchedTarget': 'matched_target_campaign_v2',
            'adGroups - matchedTarget': 'matched_target_adgroup_v2',
            'targets - matchedTarget': 'matched_target_targeting_v2'
        }
}

metrics = {
        # Sponsored Brands Video (version 3) & multi-ad group campaigns (version 4)
        'SPONSORED_BRANDS': {
            'campaigns': 'attributedConversions14d,attributedConversions14dSameSKU,attributedSales14d,attributedSales14dSameSKU,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignStatus,clicks,cost,dpv14d,impressions,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,dpv14d,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,attributedBrandedSearches14d,currency,topOfSearchImpressionShare',
            'campaigns - placement': 'attributedConversions14d,attributedConversions14dSameSKU,attributedSales14d,attributedSales14dSameSKU,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignStatus,clicks,cost,impressions,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,dpv14d,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,attributedBrandedSearches14d,currency', # removed: placement*
            'adGroups': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedSales14d,attributedSales14dSameSKU,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignStatus,clicks,cost,impressions,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,dpv14d,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,attributedBrandedSearches14d,currency',
            'targets': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedSales14d,attributedSales14dSameSKU,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignStatus,clicks,cost,impressions,targetId,targetingExpression,targetingText,targetingType,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,dpv14d,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,attributedBrandedSearches14d,currency,topOfSearchImpressionShare',
            'keywords': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedSales14d,attributedSales14dSameSKU,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignStatus,clicks,cost,impressions,keywordBid,keywordId,keywordStatus,keywordText,matchType,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,dpv14d,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,attributedBrandedSearches14d,currency,topOfSearchImpressionShare',
            # search term report
            'keywords - query': 'adGroupId,adGroupName,attributedConversions14d,attributedSales14d,campaignBudget,campaignBudgetType,campaignStatus,clicks,cost,impressions,keywordBid,keywordId,keywordStatus,keywordText,matchType,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr', # removed: query*, currency
            'ads': 'adGroupId,adGroupName,adId,applicableBudgetRuleId,applicableBudgetRuleName,attributedConversions14d,attributedConversions14dSameSKU,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSales14d,attributedSales14dSameSKU,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignRuleBasedBudget,campaignStatus,clicks,cost,dpv14d,impressions,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,attributedBrandedSearches14d,currency',
        },
        # Sponsored Display version 2
        'SPONSORED_DISPLAY': {
            'campaigns': 'attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedSalesNewToBrand14d,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,campaignBudget,campaignId,campaignName,campaignStatus,clicks,cost,costType,currency,impressions,viewAttributedConversions14d,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewImpressions,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,vtr,vctr,avgImpressionsFrequency,cumulativeReach',
            'adGroups': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,bidOptimization,campaignId,campaignName,clicks,cost,currency,impressions,viewAttributedConversions14d,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewImpressions,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,vtr,vctr,avgImpressionsFrequency,cumulativeReach',
            'productAds': 'adGroupId,adGroupName,adId,asin,attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedSalesNewToBrand14d,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,campaignId,campaignName,clicks,cost,currency,impressions,sku,viewAttributedConversions14d,viewImpressions,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,vtr,vctr,avgImpressionsFrequency,cumulativeReach',
            'targets': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedSalesNewToBrand14d,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,campaignId,campaignName,clicks,cost,currency,impressions,targetId,targetingExpression,targetingText,targetingType,viewImpressions,viewAttributedConversions14d,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,vtr,vctr',
            # 'asins': 'adGroupId,adGroupName,asin,attributedSales14dOtherSKU,attributedSales1dOtherSKU,attributedSales30dOtherSKU,attributedSales7dOtherSKU,attributedUnitsOrdered14dOtherSKU,attributedUnitsOrdered1dOtherSKU,attributedUnitsOrdered30dOtherSKU,attributedUnitsOrdered7dOtherSKU,campaignId,campaignName,currency,otherAsin,sku,viewAttributedUnitsOrdered1dOtherSKU,viewAttributedUnitsOrdered7dOtherSKU,viewAttributedUnitsOrdered14dOtherSKU,viewAttributedUnitsOrdered30dOtherSKU,viewAttributedSales1dOtherSKU,viewAttributedSales7dOtherSKU,viewAttributedSales14dOtherSKU,viewAttributedSales30dOtherSKU,viewAttributedConversions1dOtherSKU,viewAttributedConversions7dOtherSKU,viewAttributedConversions14dOtherSKU,viewAttributedConversions30dOtherSKU,attributedConversions1dOtherSKU,attributedConversions7dOtherSKU,attributedConversions14dOtherSKU,attributedConversions30dOtherSKU', # empty. keywordId, targetId not recognized
            'campaigns - matchedTarget': 'attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedSalesNewToBrand14d,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,campaignBudget,campaignId,campaignName,campaignStatus,clicks,cost,costType,currency,impressions,viewAttributedConversions14d,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewImpressions,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d', # removed: matchedTarget
            'adGroups - matchedTarget': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7dSameSKU,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,bidOptimization,campaignId,campaignName,clicks,cost,currency,impressions,viewAttributedConversions14d,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewImpressions,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d',  # removed: matchedTarget
            'targets - matchedTarget': 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedSalesNewToBrand14d,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,campaignId,campaignName,clicks,cost,currency,impressions,targetId,targetingExpression,targetingText,targetingType,viewAttributedConversions14d,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d'    # removed: matchedTarget
        }
}


def request_report(ad_product, report_type, report_date, marketplace='US'):
    """
    Requests report using SP (version 2). It can only download 1 day at a time.

    Args:
        ad_product (str): ['SPONSORED_BRANDS', 'SPONSORED_DISPLAY']
        report_type (str): ['campaigns', 'adGrouops', 'keywords' ...]
        report_date (str | dt.date)
        marketplace (str): ['US', 'CA', 'UK', 'MX']

    Returns:
        response.payload
    """
    report_name = f"{ad_product} ({marketplace}) {report_type} {report_date}"
    logger.info(f"Requesting: {report_name}")

    body = {
        "reportDate": str(report_date).replace('-', ''),
        "metrics": metrics[ad_product][report_type],
    }

    if ad_product == 'SPONSORED_BRANDS':
        body['creativeType'] = 'all'
    if ad_product == 'SPONSORED_DISPLAY':
        body['tactic'] = 'T00020'   # Targeting Strategy: Product Targeting
        if report_type == 'asins':
            body['tactic'] = 'T00030'

    # Retrieves segment 
    if ' - ' in report_type:
        report_type, segment = report_type.split(' - ')
        body['segment'] = segment

    reports = {
        'SPONSORED_BRANDS': sb_Reports,
        'SPONSORED_DISPLAY': sd_Reports
    }

    sleep_multiplier = 1
    while True:
        try:
            Reports = reports[ad_product](account=marketplace, marketplace=Marketplaces[marketplace])
            response = Reports.post_report(recordType=report_type, body=json.dumps(body))
            payload = response.payload
            return payload

        except ConnectionError as e:
            logger.error(e)
            time.sleep(2 * sleep_multiplier)
            sleep_multiplier += 0.10


def download_report(report_id, ad_product, marketplace='US', directory='', report_name=''):
    """
    Gets a requested report after requesting/posting.
    Downloads to a gzipped file if a save directory is specified. 
    Returns a pandas dataframe otherwise.

    Args:
        report_id (str)
        ad_product (str): ['SPONSORED_BRANDS', 'SPONSORED_DISPLAY']
        marketplace = ['US', 'CA', 'UK', 'MX']
        directory (str | os.path): save to file path
        report_name (str)

    Returns:
        file_path [if directory], pandas.DataFrame
    """
    download = True if directory else False
    if not os.path.exists(directory) and directory:
        logger.info(f"Creating new directory: {directory}")
        os.makedirs(directory)
        download = True

    reports = {
        'SPONSORED_BRANDS': sb_Reports,
        'SPONSORED_DISPLAY': sd_Reports
    }
    Reports = reports[ad_product](account=marketplace, marketplace=Marketplaces[marketplace])

    logger.info(f"Downloading Report {report_name}")

    for i in range(0, 20):
        response = Reports.get_report(reportId=report_id)
        status = response.payload['status']

        logger.info(f"\tReport status: {status}")
    
        if status == 'SUCCESS':
            location = response.payload['location']

            # Returns a pd dataframe
            if not download:
                result = Reports.download_report(url=location, format='data')
                data = result.payload
                df = pd.DataFrame(data)
                return df

            # Downloads the report to directory
            result = Reports.download_report(url=location, format='url')
            url = result.payload
            download_response = requests.get(url)

            if download_response.status_code == 200:
                file_path = os.path.join(directory, report_name + '.json.gz')

                with open(file_path, 'wb') as file:
                    file.write(download_response.content)

                logger.info("File downloaded successfully.")
                return file_path

            else:
                logger.info("Failed to download file.")
                logger.info("\tRedownloading...")

        time.sleep(30)

    raise Exception("Download failed: max waiting time reached")



def request_download_reports(ad_product, report_type, marketplace, start_date, end_date, root_directory):
    """
    Streamlines the process of downloading reports.

    Args:
        ad_product (str): ['SPONSORED_BRANDS', 'SPONSORED_DISPLAY']
        report_type (str): ['campaigns', 'adGrouops', 'keywords' ...]
        marketplace (str): ['US', 'CA', 'UK', 'MX']
        start_date, end_date (str | dt.date)
        root_directory (str | os.path): It will automatically create subfolders from the root directory.

    Returns:
        file_paths (list): list of paths saved to.
    """
    directory = os.path.join(root_directory, ad_product, report_type, marketplace)
    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d').date()

    report_ids = {}

    # request reports
    current_date = start_date
    while current_date <= end_date:
        try:
            report_name = f"{ad_product} ({marketplace}) {report_type} {current_date}"
            response = request_report(ad_product, report_type, current_date, marketplace)
            report_ids[report_name] = response['reportId']
            current_date += dt.timedelta(days=1)
        except AdvertisingApiTooManyRequestsException as error:
            logger.error(error)
            time.sleep(5)

    file_paths = []
    # download multiple reports
    for report_name in report_ids:
        file_path = download_report(report_ids[report_name], ad_product, marketplace, directory, report_name)
        file_paths.append(file_path)

    return file_paths


def combine_data(directory=None, file_paths=[], file_extension='.json.gz'):
    """
    Combines files in a directory and/or in file_paths.
    Inserts `marketplace` and `date`.
    Returns pandas dataframe.
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


def update_data(ad_product, report_type, start_date, end_date, marketplaces=['US', 'CA', 'UK']):
    """
    Adds upsert data step to request_download_reports by
    combining the downloaded files
    """
    gzipped_directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')

    for marketplace in to_list(marketplaces):
        # request & download reports
        file_paths = request_download_reports(ad_product, report_type, marketplace, start_date, end_date, gzipped_directory)

        # combine reports
        combined_data = combine_data(file_paths=file_paths)
    
        # upserts to db
        table_name = f"{ad_product.lower()}.{table_names[ad_product][report_type]}"
        postgresql.upsert_bulk(table_name, combined_data, file_extension='pandas')


def update_all_data(start_date, end_date, ad_products = ['SPONSORED_BRANDS', 'SPONSORED_DISPLAY'], marketplaces=['US', 'CA', 'UK']):
    gzipped_directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')

    for ad_product in to_list(ad_products):

        for report_type in metrics[ad_product]:
            update_data(ad_product, report_type, start_date, end_date, marketplaces)


def create_table(directory, drop_table_if_exists=False):
    """
    Creates table by iterating all files in a directory & combining data.
    Upserts data by default.
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
    start_date, end_date = dt.date.today() - dt.timedelta(days=20), dt.date.today()
    update_all_data(start_date, end_date)