import requests
import json
import pandas as pd
import datetime as dt
import io
import gzip
import time
from ad_api.api.reports import Reports
from ad_api.base import Marketplaces


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
        'campaign': f'{campaign_addtl_metrics}, {campaign_base_metrics}',
        'adGroup': f'{adGroup_addtl_metrics}, {campaign_base_metrics}',
        'campaign, adGroup': f'{campaign_addtl_metrics}, {adGroup_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
        'campaignPlacement': f'{campaignPlacement_addtl_metrics}, {campaign_base_metrics}',     # useless? No campaignIds/adGroupIds
        'campaign, campaignPlacement': f'{campaign_addtl_metrics}, {campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
        'adGroup, campaignPlacement': f'{campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '), # can't add adGroup addt'l metrics == campaignPlacement
        'campaign, adGroup, campaignPlacement': f'{campaign_addtl_metrics}, {campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '), # Can't add adGrouop add'tl metrics; == campaign, campaignPlacement
        'targeting': f'{targeting_base_metrics}, {targeting_addtl_metrics}',
        'searchTerm': searchTerm_base_metrics,
        'advertiser': advertiser_base_metrics,
        'asin': asin_base_metrics
    },
    # Sponsored Brands Video (version 3)
    'SPONSORED_BRANDS': {
        'purchasedAsin': purchasedAsin_base_metrics
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
        "name": f"{ad_product} ({marketplace}) {report_type_id} {str(group_by.split(', '))} {start_date} - {end_date}",
        "startDate": start_date,
        "endDate": end_date,
        "configuration":{
            "adProduct": ad_product,
            "groupBy": group_by.split(', '),    # converts to list
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
    
    response = Reports(account=marketplace, marketplace=Marketplaces[marketplace]).post_report(body=body)
    payload = response.payload
    return payload


def download_report(report_id, directory, report_name, marketplace='US'):
    """
    Once you have made a successful POST call, report generation can take up to three hours.
    You can check the report generation status by using the reportId returned in the initial 
    request to call the GET report endpoint: GET /reporting/reports/{reportId}.
    If the report is still generating, status is set to PENDING or PROCESSING.
    When your report is ready to download, status returns as COMPLETED, and you will see an 
    address in the url field.

    Args:
        report_id (str): id of the report to be downloaded after posting a request report

    Returns:
        file_path (str)
    """
    while True:
        print(f"Downloading {report_name}")
        response = Reports(accounts=marketplace, marketplace=Marketplaces[marketplace]).get_report(reportId=report_id)
        status, url = response.payload['status'], response.payload['url']
        print(f"\tReport status: {status}")

        if status == 'COMPLETED':
            # Download the report
            response = requests.get(url)

            if response.status_code == 200:
                file_path = os.path.join(directory, report_name + '.json.gz')

                with open(file_path, 'wb') as file:
                    file.write(response.content)

                print("File downloaded successfully.")

                return file_path

            else:
                print("Failed to download file.")
                print("\tRedownloading...")

        time.sleep(30)


def request_download_reports(ad_product, report_type_id, group_by, start_date, end_date, time_unit='DAILY', marketplace='US'):
    """Streamlines the process of downloading reports."""
    response = request_report(ad_product, report_type_id, group_by, start_date, end_date, time_unit, marketplace)

    report_id, report_name = response['reportId'], response['name']

    file_path = download_report(report_id, directory, report_name, marketplace)

    return file_path


def update_data():
    """
    Download reports & upserts to database
    """
    

if __name__ == '__main__':
    import postgresql
    import time
    import re
    reports = [
        ['spCampaigns', 'campaign'], # sponsored_products_campaign
        ['spCampaigns', 'adGroup'],  # sponsored_products_adgroup (useless?)
        ['spCampaigns', 'campaignPlacement'], # sponored_products_placement (useless?)
        ['spCampaigns', 'campaign, adGroup'], # sponsored_products_campaign_adgroup
        ['spCampaigns', 'campaign, campaignPlacement'], # sponsored_products_campaign_placement
        ['spCampaigns', 'adGroup, campaignPlacement'], # sponsored_products_adgroup_placement
        ['spCampaigns', 'campaign, adGroup, campaignPlacement'], # sponsored_products_campaign_adgroup_placement (most useful?)
        ['spTargeting', 'targeting'], # sponsored_products_targeting
        ['spSearchTerm', 'searchTerm'], # sponsored_products_search_term_report
        ['spAdvertisedProduct', 'advertiser'], # sponsored_products_advertised_product
        ['spPurchasedProduct', 'asin']         # sponsored_products_purchased_product
    ]
    # request reports
    report_ids = {}
    for report in reports:
        try:
            report_type, group_by = report[0], report[1]
            start_date, end_date = '2023-07-01', '2023-07-25'
            response = request_report('SPONSORED_PRODUCTS', report_type, group_by, start_date, end_date, martketplace='US')
            report_ids[response['name']] = response['reportId']
        except Exception as e:
            print(e)
    time.sleep(60)

    # download reports
    for report in report_ids:
        print(f"Downloading Report {report}")
        filename = report
        report_id = report_ids[report]
        directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')
        file_path = download_report(report_id, directory, filename, 'US')

        # insert to db
        report_names = {
            "['campaign']": "sponsored_products.campaign",
            "['adGroup']":  "sponsored_products.adgroup", #(useless?)
            # "['campaignPlacement']": "sponsored_products.placement", # USELESS no pkey (campaignId/adgroupId)
            "['campaign', 'adGroup']": "sponsored_products.campaign_adgroup",
            "['campaign', 'campaignPlacement']": "sponsored_products.campaign_placement",
            "['adGroup', 'campaignPlacement']": "sponsored_products.adgroup_placement", # can't add adgroup additional metrics (==campaign_placement)
            "['campaign', 'adGroup', 'campaignPlacement']": "sponsored_products.campaign_adgroup_placement", # cannot add adgroup additional metrics (==campaign_placement)
            "['targeting']": "sponsored_products.targeting",
            "['searchTerm']": "sponsored_products.search_term",
            "['advertiser']": "sponsored_products.advertised_product",
            "['asin']": "sponsored_products.purchased_product",
            "['purchasedAsin']": "sponsored_brands.purchased_product"
        }

        with postgresql.setup_cursor() as cur:
            # groupby = re.findall(r"(\[.*\])", filename)[0]
            # table_name = report_names[str(groupby)]
            # print("DROPPING TABLE")
            # cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            # print(f"Creating table {table_name}")
            # create_table(cur, file_path, file_extension='json', table_name=table_name)
            # print("Updating triggers")
            # update_updated_at_trigger(cur, table_name)
            print("Upserting bulk")
            upsert_bulk(table_name=table_name, file_path=file_path)