import requests
import json
import pandas as pd
import datetime as dt
import io
import gzip


amazon_ads_api_config_path = "amazon_ads_api_env.json"
with open(amazon_ads_api_config_path) as f:
    config = json.load(f)

# Constuct all columns by `group_by`
campaign_base_metrics = 'impressions, clicks, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, date, startDate, endDate, campaignBiddingStrategy, costPerClick, clickThroughRate, spend'
campaign_addtl_metrics = 'campaignName, campaignId, campaignStatus, campaignBudgetAmount, campaignBudgetType, campaignRuleBasedBudgetAmount, campaignApplicableBudgetRuleId, campaignApplicableBudgetRuleName, campaignBudgetCurrencyCode, topOfSearchImpressionShare'
adGroup_addtl_metrics = 'adGroupName, adGroupId, adStatus'
campaignPlacement_addtl_metrics = 'placementClassification'
targeting_base_metrics = 'impressions, clicks, costPerClick, clickThroughRate, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, salesOtherSku7d, unitsSoldOtherSku7d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d, keywordId, keyword, campaignBudgetCurrencyCode, date, startDate, endDate, portfolioId, campaignName, campaignId, campaignBudgetType, campaignBudgetAmount, campaignStatus, keywordBid, adGroupName, adGroupId, keywordType, matchType, targeting, topOfSearchImpressionShare'
targeting_addtl_metrics = 'adKeywordStatus'
searchTerm_base_metrics = 'impressions, clicks, costPerClick, clickThroughRate, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, salesOtherSku7d, unitsSoldOtherSku7d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d, keywordId, keyword, campaignBudgetCurrencyCode, date, startDate, endDate, portfolioId, searchTerm, campaignName, campaignId, campaignBudgetType, campaignBudgetAmount, campaignStatus, keywordBid, adGroupName, adGroupId, keywordType, matchType, targeting, adKeywordStatus'
advertiser_base_metrics = 'date, startDate, endDate, campaignName, campaignId, adGroupName, adGroupId, adId, portfolioId, impressions, clicks, costPerClick, clickThroughRate, cost, spend, campaignBudgetCurrencyCode, campaignBudgetAmount, campaignBudgetType, campaignStatus, advertisedAsin, advertisedSku, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, salesOtherSku7d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, unitsSoldOtherSku7d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d'
asin_base_metrics = 'date, startDate, endDate, portfolioId, campaignName, campaignId, adGroupName, adGroupId, keywordId, keyword, keywordType, advertisedAsin, purchasedAsin, advertisedSku, campaignBudgetCurrencyCode, matchType, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, purchases1d, purchases7d, purchases14d, purchases30d, unitsSoldOtherSku1d, unitsSoldOtherSku7d, unitsSoldOtherSku14d, unitsSoldOtherSku30d, salesOtherSku1d, salesOtherSku7d, salesOtherSku14d, salesOtherSku30d, purchasesOtherSku1d, purchasesOtherSku7d, purchasesOtherSku14d, purchasesOtherSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d'

columns = {
    'campaign': f'{campaign_addtl_metrics}, {campaign_base_metrics}',
    'adGroup': f'{adGroup_addtl_metrics}, {campaign_base_metrics}',
    'campaign, adGroup': f'{campaign_addtl_metrics}, {adGroup_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
    'campaignPlacement': f'{campaignPlacement_addtl_metrics}, {campaign_base_metrics}',
    'campaign, campaignPlacement': f'{campaign_addtl_metrics}, {campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
    'adGroup, campaignPlacement': f'{campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
    'campaign, adGroup, campaignPlacement': f'{campaign_addtl_metrics}, {campaignPlacement_addtl_metrics}, {campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
    'targeting': f'{targeting_base_metrics}, {targeting_addtl_metrics}',
    'searchTerm': searchTerm_base_metrics,
    'advertiser': advertiser_base_metrics,
    'asin': asin_base_metrics
}

filters = {
    'spSearchTerm': ['TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED'],
    'spPurchasedProduct': ['BROAD', 'PHRASE', 'EXACT', 'TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED']
}


class AmazonAdvertisingReports():
    def __init__(self, client_id, client_secret, refresh_token, profile_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.profile_id = profile_id
        self.access_token = None
        self.expires_in = None

    def get_access_token(self):
        """Retrieves access token using refresh token.
            Access token expires every hour."""
        print("Getting access_token")
        if self.access_token and self.expires_in > dt.datetime.today():
            return self.access_token
        url = 'https://api.amazon.com/auth/o2/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        response = requests.post(url, data=data)
        response_json = response.json()
        self.access_token = response_json['access_token']
        self.expires_in = dt.datetime.today() + dt.timedelta(seconds=response_json['expires_in'])
        return self.access_token

    def request_report(self, marketplace, ad_product, report_type_id, group_by, start_date, end_date, time_unit='DAILY'):
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
            marketplace (str): ['us', 'ca']. It will retrieve the profile_id of the marketplace
            ad_product (str): ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"]
            report_type_id (str): ['spCampaigns', 'spTargeting', 'spSearchTerm', 'spAdvertisedProduct', 'spPurchasedProduct', 'sbPurchasedProduct']
            group_by (str): will convert into a list spCampaigns: [campaign, adGroup, ad, productAds, placement, category, asin]
        Returns:
            requests.response
        """
        # Access token
        if self.access_token is None or self.expires_in > dt.datetime.today():
            self.get_access_token()

        # Selects date columns as per time unit
        for col in columns:
            if time_unit == 'DAILY':
                columns[col] = columns[col].replace(' startDate, endDate,', '')
            elif time_unit == 'SUMMARY':
                columns[col] = columns[col].replace(' date,', '')

        # Post API
        url = 'https://advertising-api.amazon.com/reporting/reports'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Amazon-Advertising-API-ClientId': self.client_id,
            'Amazon-Advertising-API-Scope': self.profile_id[marketplace]
        }
        body = {
            "name": f"{ad_product} ({marketplace.upper()}) {report_type_id} {str(group_by.split(', '))} {start_date} - {end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "date": start_date,
            "configuration":{
                "adProduct": ad_product,
                "groupBy": group_by.split(', '),    # converts to list
                "columns": columns[group_by].split(', '),
                "filters": [
                    {
                        "field": "keywordType",
                        "values": filters[report_type_id]
                    }
                ],
                "reportTypeId": report_type_id,
                "timeUnit": time_unit,
                "format": "GZIP_JSON"
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response_json = response.json()
        print(response.text)
        report_ids[response_json['name']] = response_json['configuration']['reportTypeId']
        return response_json


    def check_report_status(self, report_id, marketplace):
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
            response.requests
        """
                # Access token
        if self.access_token is None or self.expires_in > dt.datetime.today():
            self.get_access_token()
            url = f'https://advertising-api.amazon.com/reporting/reports/{report_id}'

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Amazon-Advertising-API-ClientId': self.client_id,
            'Amazon-Advertising-API-Scope': self.profile_id[marketplace]
        }
        response = requests.get(url, headers=headers)
        response_json = response.json()
        print(response.text)
        return response_json


    def download_report(self, url, directory, report_name):
        """
        Downloads the .json.gz file report
        """
        # Download the report
        response = requests.get(url)
        # Save it to .json.gz zip file
        if response.status_code == 200:
            file_path = os.path.join(directory, report_name + '.json.gz')
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print("File downloaded successfully.")
            print(report_name)
        else:
            print("Failed to download file.")


def unzip_to_memory(file_path):
    """
    Unzips a gzip-compressed file into memory.

    Args:
        file_path (str): The path to the gzip-compressed file.

    Returns:
        file_like_object (_io.BytesIO): The uncompressed file content in memory.
    """
    with open(file_path, 'rb') as file:
        file_content = file.read()
    # Create an in-memory stream from the file content
    file_stream = io.BytesIO(file_content)
    # Create a gzip file object
    gzip_file = gzip.GzipFile(fileobj=file_stream)
    # Read the uncompressed content from the gzip file object
    uncompressed_content = gzip_file.read()
    file_like_object = io.BytesIO(uncompressed_content)  # Use io.BytesIO for binary content
    return file_like_object


if __name__ == '__main__':
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

    report_ids = {}
    for report in reports:
        try:
            report_type, group_by = report[0], report[1]
            response = request_report(config['access_token'], 'us', 'SPONSORED_PRODUCTS', report_type, group_by, start_date='2023-04-02', end_date='2023-04-30')
            report_ids[response['name']] = response['reportId']
        except Exception as e:
            print(e)

    for report in report_ids:
        report_id = report_ids[report]
        response = check_report_status(report_id, 'us')
        download_report(response['url'], os.path.join(os.getcwd(), 'PPC Data', 'RAW Gzipped JSON Reports'), report)