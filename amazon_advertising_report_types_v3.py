"""
Amazon Advertising Reporting Version 3 for Sponsored Products, Brands & Display

https://advertising.amazon.com/API/docs/en-us/guides/reporting/v3/report-types/overview
"""


# Sponsored Products metrics
sp_campaign_base_metrics = 'impressions, clicks, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, date, startDate, endDate, campaignBiddingStrategy, costPerClick, clickThroughRate, spend'
sp_campaign_addtl_metrics = 'campaignName, campaignId, campaignStatus, campaignBudgetAmount, campaignBudgetType, campaignRuleBasedBudgetAmount, campaignApplicableBudgetRuleId, campaignApplicableBudgetRuleName, campaignBudgetCurrencyCode, topOfSearchImpressionShare'
sp_adGroup_addtl_metrics = 'adGroupName, adGroupId, adStatus'
sp_campaignPlacement_addtl_metrics = 'placementClassification'
sp_targeting_base_metrics = 'impressions, clicks, costPerClick, clickThroughRate, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, salesOtherSku7d, unitsSoldOtherSku7d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d, keywordId, keyword, campaignBudgetCurrencyCode, date, startDate, endDate, portfolioId, campaignName, campaignId, campaignBudgetType, campaignBudgetAmount, campaignStatus, keywordBid, adGroupName, adGroupId, keywordType, matchType, targeting, topOfSearchImpressionShare'
sp_targeting_addtl_metrics = 'adKeywordStatus'
sp_searchTerm_base_metrics = 'impressions, clicks, costPerClick, clickThroughRate, cost, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, salesOtherSku7d, unitsSoldOtherSku7d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d, keywordId, keyword, campaignBudgetCurrencyCode, date, startDate, endDate, portfolioId, searchTerm, campaignName, campaignId, campaignBudgetType, campaignBudgetAmount, campaignStatus, keywordBid, adGroupName, adGroupId, keywordType, matchType, targeting, adKeywordStatus'
sp_advertiser_base_metrics = 'date, startDate, endDate, campaignName, campaignId, adGroupName, adGroupId, adId, portfolioId, impressions, clicks, costPerClick, clickThroughRate, cost, spend, campaignBudgetCurrencyCode, campaignBudgetAmount, campaignBudgetType, campaignStatus, advertisedAsin, advertisedSku, purchases1d, purchases7d, purchases14d, purchases30d, purchasesSameSku1d, purchasesSameSku7d, purchasesSameSku14d, purchasesSameSku30d, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, attributedSalesSameSku1d, attributedSalesSameSku7d, attributedSalesSameSku14d, attributedSalesSameSku30d, salesOtherSku7d, unitsSoldSameSku1d, unitsSoldSameSku7d, unitsSoldSameSku14d, unitsSoldSameSku30d, unitsSoldOtherSku7d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d, acosClicks7d, acosClicks14d, roasClicks7d, roasClicks14d'
sp_asin_base_metrics = 'date, startDate, endDate, portfolioId, campaignName, campaignId, adGroupName, adGroupId, keywordId, keyword, keywordType, advertisedAsin, purchasedAsin, advertisedSku, campaignBudgetCurrencyCode, matchType, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, purchases1d, purchases7d, purchases14d, purchases30d, unitsSoldOtherSku1d, unitsSoldOtherSku7d, unitsSoldOtherSku14d, unitsSoldOtherSku30d, salesOtherSku1d, salesOtherSku7d, salesOtherSku14d, salesOtherSku30d, purchasesOtherSku1d, purchasesOtherSku7d, purchasesOtherSku14d, purchasesOtherSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d'

# Sponsored Brands metrics
sb_purchasedAsin_base_metrics = 'campaignId, adGroupId, date, startDate, endDate, campaignBudgetCurrencyCode, campaignName, adGroupName, attributionType, purchasedAsin, productName, productCategory, sales14d, orders14d, unitsSold14d, newToBrandSales14d, newToBrandPurchases14d, newToBrandUnitsSold14d, newToBrandSalesPercentage14d, newToBrandPurchasesPercentage14d, newToBrandUnitsSoldPercentage14d'
sb_campaign_base_metrics = 'addToCart, addToCartClicks, addToCartRate, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, topOfSearchImpressionShare, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewableImpressions, viewClickThroughRate'
sb_campaign_addtl_metrics = 'campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, topOfSearchImpressionShare' # Duplicate
sb_adgroup_base_metrics = 'addToCart, addToCartClicks, addToCartRate, adGroupId, adGroupName, adStatus, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate'
sb_campaignPlacement_base_metrics = 'addToCart, addToCartClicks, addToCartRate, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewableImpressions, viewClickThroughRate'
sb_campaignPlacement_addtl_metrics = 'placementClassification'

# Sponsored Display metrics
sd_campaign_base_metrics = 'addToCart, addToCartClicks, addToCartRate, addToCartViews, brandedSearches, brandedSearchesClicks, brandedSearchesViews, brandedSearchRate, campaignBudgetCurrencyCode, campaignId, campaignName, clicks, cost, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, eCPBrandSearch, endDate, impressions, impressionsViews, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandSalesClicks, newToBrandUnitsSold, newToBrandUnitsSoldClicks, purchases, purchasesClicks, purchasesPromotedClicks, sales, salesClicks, salesPromotedClicks, startDate, unitsSold, unitsSoldClicks, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewClickThroughRate'
sd_campaign_addtl_metrics = 'campaignBudgetAmount, campaignStatus, costType, cumulativeReach, impressionsFrequencyAverage, newToBrandDetailPageViewClicks, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewViews, newToBrandECPDetailPageView, newToBrandSales'
sd_adgroup_base_metrics = 'addToCart, addToCartClicks, addToCartRate, addToCartViews, adGroupId, adGroupName, bidOptimization, brandedSearches, brandedSearchesClicks, brandedSearchesViews, brandedSearchRate, campaignBudgetCurrencyCode, campaignId, campaignName, clicks, cost, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, eCPBrandSearch, endDate, impressions, impressionsViews, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandSales, newToBrandSalesClicks, newToBrandUnitsSold, newToBrandUnitsSoldClicks, purchases, purchasesClicks, purchasesPromotedClicks, sales, salesClicks, salesPromotedClicks, startDate, unitsSold, unitsSoldClicks, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewClickThroughRate'
sd_adgroup_addtl_metrics = 'cumulativeReach, impressionsFrequencyAverage, newToBrandDetailPageViewClicks, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewViews, newToBrandECPDetailPageView'
sd_matched_target_addtl_metrics = 'matchedTargetAsin'

# ad_type -> reportTypeId -> groupBy -> [table_name, metrics]
table_names = {
        "SPONSORED_PRODUCTS": {
            "spCampaigns": {
                "['campaign']": "campaign",
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
            "sbCampaigns": {
                "['campaign']": "campaign"
            },
            "sbAdGroup": {
                "['adGroup']": "adgroup"
            },
            "sbPurchasedProduct": {
                "['purchasedAsin']": "purchased_product"
            },
            "sbCampaignPlacement": {
                "['campaign']": "campaign_placement"
            }
        },
        "SPONSORED_DISPLAY": {
            "sdCampaigns": {
                "['campaign']": "campaign",
                "['campaign', 'matchedTarget']": "matched_target"
            },
            "sdAdGroup": {
                "['adGroup']": "adgroup",
                "['adGroup', 'matchedTarget']": "matched_target"
            }
        }
}




# Sponsored ads group by metrics
metrics = {
    # Sponsored Products (version 3)
    "SPONSORED_PRODUCTS": {
        "['campaign']": f'{sp_campaign_addtl_metrics}, {sp_campaign_base_metrics}',
        "['campaign', 'adGroup']": f'{sp_campaign_addtl_metrics}, {sp_adGroup_addtl_metrics}, {sp_campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
        "['campaignPlacement']": f'{sp_campaignPlacement_addtl_metrics}, {sp_campaign_base_metrics}',     # useless? No campaignIds/adGroupIds
        "['campaign', 'campaignPlacement']": f'{sp_campaign_addtl_metrics}, {sp_campaignPlacement_addtl_metrics}, {sp_campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '),
        "['adGroup', 'campaignPlacement']": f'{sp_campaignPlacement_addtl_metrics}, {sp_campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '), # can't add adGroup addt'l metrics == campaignPlacement
        "['campaign', 'adGroup', 'campaignPlacement']": f'{sp_campaign_addtl_metrics}, {sp_campaignPlacement_addtl_metrics}, {sp_campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', '), # Can't add adGrouop add'tl metrics; == campaign, campaignPlacement
        "['targeting']": f'{sp_targeting_base_metrics}, {sp_targeting_addtl_metrics}',
        "['searchTerm']": sp_searchTerm_base_metrics,
        "['advertiser']": sp_advertiser_base_metrics,
        "['asin']": sp_asin_base_metrics
    },
    # Sponsored Brands Video (version 3)
    "SPONSORED_BRANDS": {
        "['campaign']": sb_campaign_base_metrics, # Additional metrics already in base metrics
        "['adGroup']": sb_adgroup_base_metrics,
        # "['campaign']": 


        "['purchasedAsin']": sb_purchasedAsin_base_metrics
    },
    "SPONSORED_DISPLAY": {
        "['campaign']": f'{sd_campaign_base_metrics}, {sd_campaign_addtl_metrics}',
        "['campaign', 'matchedTarget']": f'{sd_campaign_base_metrics}, {sd_campaign_addtl_metrics}, {sd_matched_target_addtl_metrics}',
        "['adGroup']": f'{sd_adgroup_base_metrics}, {sd_adgroup_addtl_metrics}',
        "['adGroup', 'matchedTarget']": f'{sd_adgroup_base_metrics}, {sd_adgroup_addtl_metrics}, {sd_matched_target_addtl_metrics}'
    }
}


filters = {
    'spSearchTerm': ['TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED'],
    'spPurchasedProduct': ['BROAD', 'PHRASE', 'EXACT', 'TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED']
}