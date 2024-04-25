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
sp_purchasedProduct_base_metrics = 'date, startDate, endDate, portfolioId, campaignName, campaignId, adGroupName, adGroupId, keywordId, keyword, keywordType, advertisedAsin, purchasedAsin, advertisedSku, campaignBudgetCurrencyCode, matchType, unitsSoldClicks1d, unitsSoldClicks7d, unitsSoldClicks14d, unitsSoldClicks30d, sales1d, sales7d, sales14d, sales30d, purchases1d, purchases7d, purchases14d, purchases30d, unitsSoldOtherSku1d, unitsSoldOtherSku7d, unitsSoldOtherSku14d, unitsSoldOtherSku30d, salesOtherSku1d, salesOtherSku7d, salesOtherSku14d, salesOtherSku30d, purchasesOtherSku1d, purchasesOtherSku7d, purchasesOtherSku14d, purchasesOtherSku30d, kindleEditionNormalizedPagesRead14d, kindleEditionNormalizedPagesRoyalties14d'

# Sponsored Brands metrics
sb_campaign_base_metrics = 'addToCart, addToCartClicks, addToCartRate, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, topOfSearchImpressionShare, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewableImpressions, viewClickThroughRate'
sb_campaign_addtl_metrics = 'campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, topOfSearchImpressionShare' # Duplicate
sb_adgroup_base_metrics = 'addToCart, addToCartClicks, addToCartRate, adGroupId, adGroupName, adStatus, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate'
sb_campaignPlacement_base_metrics = 'addToCart, addToCartClicks, addToCartRate, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewableImpressions, viewClickThroughRate'
sb_campaignPlacement_addtl_metrics = 'placementClassification'
sb_targeting_base_metrics = 'addToCart, addToCartClicks, addToCartRate, adGroupId, adGroupName, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, keywordBid, keywordId, adKeywordStatus, keywordText, keywordType, matchType, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, targetingExpression, targetingId, targetingText, targetingType, topOfSearchImpressionShare'
sb_searchTerm_base_metrics = 'adGroupId, adGroupName, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, endDate, impressions, keywordBid, keywordId, keywordText, matchType, purchases, purchasesClicks, sales, salesClicks, searchTerm, startDate, unitsSold, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewableImpressions, viewClickThroughRate'
sb_searchTerm_addtl_metrics = 'adKeywordStatus'
sb_ad_base_metrics = 'addToCart, addToCartClicks, addToCartRate, adGroupId, adGroupName, adId, brandedSearches, brandedSearchesClicks, campaignBudgetAmount, campaignBudgetCurrencyCode, campaignBudgetType, campaignId, campaignName, campaignStatus, clicks, cost, costType, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, endDate, impressions, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewsClicks, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandPurchasesPercentage, newToBrandPurchasesRate, newToBrandSales, newToBrandSalesClicks, newToBrandSalesPercentage, newToBrandUnitsSold, newToBrandUnitsSoldClicks, newToBrandUnitsSoldPercentage, purchases, purchasesClicks, purchasesPromoted, sales, salesClicks, salesPromoted, startDate, unitsSold, unitsSoldClicks, video5SecondViewRate, video5SecondViews, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewableImpressions'
# changed name from purchasedAsin to purchasedProduct
sb_purchasedProduct_base_metrics = 'campaignId, adGroupId, date, startDate, endDate, campaignBudgetCurrencyCode, campaignName, adGroupName, attributionType, purchasedAsin, productName, productCategory, sales14d, orders14d, unitsSold14d, newToBrandSales14d, newToBrandPurchases14d, newToBrandUnitsSold14d, newToBrandSalesPercentage14d, newToBrandPurchasesPercentage14d, newToBrandUnitsSoldPercentage14d'


# Sponsored Display metrics
sd_campaign_base_metrics = 'addToCart, addToCartClicks, addToCartRate, addToCartViews, brandedSearches, brandedSearchesClicks, brandedSearchesViews, brandedSearchRate, campaignBudgetCurrencyCode, campaignId, campaignName, clicks, cost, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, eCPBrandSearch, endDate, impressions, impressionsViews, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandSalesClicks, newToBrandUnitsSold, newToBrandUnitsSoldClicks, purchases, purchasesClicks, purchasesPromotedClicks, sales, salesClicks, salesPromotedClicks, startDate, unitsSold, unitsSoldClicks, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewClickThroughRate'
sd_campaign_addtl_metrics = 'campaignBudgetAmount, campaignStatus, costType, cumulativeReach, impressionsFrequencyAverage, newToBrandDetailPageViewClicks, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewViews, newToBrandECPDetailPageView, newToBrandSales'
sd_campaign_matchedTarget_addtl_metrics = 'campaignBudgetAmount, campaignStatus, costType'
sd_adgroup_base_metrics = 'addToCart, addToCartClicks, addToCartRate, addToCartViews, adGroupId, adGroupName, bidOptimization, brandedSearches, brandedSearchesClicks, brandedSearchesViews, brandedSearchRate, campaignBudgetCurrencyCode, campaignId, campaignName, clicks, cost, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, eCPBrandSearch, endDate, impressions, impressionsViews, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandSales, newToBrandSalesClicks, newToBrandUnitsSold, newToBrandUnitsSoldClicks, purchases, purchasesClicks, purchasesPromotedClicks, sales, salesClicks, salesPromotedClicks, startDate, unitsSold, unitsSoldClicks, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewClickThroughRate'
sd_adgroup_addtl_metrics = 'cumulativeReach, impressionsFrequencyAverage, newToBrandDetailPageViewClicks, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewViews, newToBrandECPDetailPageView'
sd_matchedTarget_addtl_metrics = 'matchedTargetAsin'
sd_targeting_base_metrics = 'addToCart, addToCartClicks, addToCartRate, addToCartViews, adGroupId, adGroupName, brandedSearches, brandedSearchesClicks, brandedSearchesViews, brandedSearchRate, campaignBudgetCurrencyCode, campaignId, campaignName, clicks, cost, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, eCPBrandSearch, endDate, impressions, impressionsViews, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandSales, newToBrandSalesClicks, newToBrandUnitsSold, newToBrandUnitsSoldClicks, purchases, purchasesClicks, purchasesPromotedClicks, sales, salesClicks, salesPromotedClicks, startDate, targetingExpression, targetingId, targetingText, unitsSold, unitsSoldClicks, videoCompleteViews'
sd_targeting_addtl_metrics = 'adKeywordStatus, newToBrandDetailPageViewClicks, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewViews, newToBrandECPDetailPageView'
sd_advertisedProduct_base_metrics = 'addToCart, addToCartClicks, addToCartRate, addToCartViews, adGroupId, adGroupName, adId, bidOptimization, brandedSearches, brandedSearchesClicks, brandedSearchesViews, brandedSearchRate, campaignBudgetCurrencyCode, campaignId, campaignName, clicks, cost, cumulativeReach, date, detailPageViews, detailPageViewsClicks, eCPAddToCart, eCPBrandSearch, endDate, impressions, impressionsFrequencyAverage, impressionsViews, newToBrandDetailPageViewClicks, newToBrandDetailPageViewRate, newToBrandDetailPageViews, newToBrandDetailPageViewViews, newToBrandECPDetailPageView, newToBrandPurchases, newToBrandPurchasesClicks, newToBrandSales, newToBrandSalesClicks, newToBrandUnitsSold, newToBrandUnitsSoldClicks, promotedAsin, promotedSku, purchases, purchasesClicks, purchasesPromotedClicks, sales, salesClicks, salesPromotedClicks, startDate, unitsSold, unitsSoldClicks, videoCompleteViews, videoFirstQuartileViews, videoMidpointViews, videoThirdQuartileViews, videoUnmutes, viewabilityRate, viewClickThroughRate'
sd_purchasedProduct_base_metrics = 'adGroupId, adGroupName, asinBrandHalo, campaignBudgetCurrencyCode, campaignId, campaignName, conversionsBrandHalo, conversionsBrandHaloClicks, date, endDate, promotedAsin, promotedSku, salesBrandHalo, salesBrandHaloClicks, startDate, unitsSoldBrandHalo, unitsSoldBrandHaloClicks'

# Gross and invalid traffic reports
gross_and_invalid_metrics = 'campaignName, campaignStatus, clicks, date, startDate, endDate, grossClickThroughs, grossImpressions, impressions, invalidClickThroughRate, invalidClickThroughs, invalidImpressionRate, invalidImpressions'


# ad_type -> reportTypeId -> groupBy -> {table_name, metrics}
table_names = {
        "SPONSORED_PRODUCTS": {
            "spCampaigns": {
                "['campaign']": {
                        "table_name": "campaign",
                        "metrics": f'{sp_campaign_addtl_metrics}, {sp_campaign_base_metrics}'
                },
                "['campaign', 'adGroup']": {
                        "table_name": "campaign_adgroup",
                        "metrics": f'{sp_campaign_addtl_metrics}, {sp_adGroup_addtl_metrics}, {sp_campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', ')
                },
                "['campaign', 'campaignPlacement']": {
                        "table_name": "campaign_placement",
                        "metrics": f'{sp_campaign_addtl_metrics}, {sp_campaignPlacement_addtl_metrics}, {sp_campaign_base_metrics}'.replace(', topOfSearchImpressionShare, ', ', ')
                },
                # "['adGroup', 'campaignPlacement']": "adgroup_placement", # can't add adgroup additional metrics (==campaign_placement)
                # "['campaign', 'adGroup', 'campaignPlacement']": "campaign_adgroup_placement", # cannot add adgroup additional metrics (==campaign_placement)
            },
            "spTargeting": {
                "['targeting']": {
                        "table_name": "targeting",
                        "metrics": f'{sp_targeting_base_metrics}, {sp_targeting_addtl_metrics}'
                },
            },
            "spSearchTerm": {
                "['searchTerm']": {
                        "table_name": "search_term",
                        "metrics": sp_searchTerm_base_metrics
                },
            },
            "spAdvertisedProduct": {
                "['advertiser']": {
                        "table_name": "advertised_product",
                        "metrics": sp_advertiser_base_metrics
                },
            },
            "spPurchasedProduct": {
                "['asin']": {
                        "table_name": "purchased_product",
                        "metrics": sp_purchasedProduct_base_metrics
                },
            },
            "spGrossAndInvalids": {
                "['campaign']": {
                    "table_name": "gross_and_invalid",
                    "metrics": gross_and_invalid_metrics
                }
            }
        },
        "SPONSORED_BRANDS": {
            "sbCampaigns": {
                "['campaign']": {
                        "table_name": "campaign",
                        "metrics": sb_campaign_base_metrics
                },
            },
            "sbAdGroup": {
                "['adGroup']": {
                        "table_name": "adgroup",
                        "metrics": sb_adgroup_base_metrics
                },
            },
            "sbCampaignPlacement": {
                "['campaignPlacement']": {
                        "table_name": "campaign_placement",
                        "metrics": f'{sb_campaignPlacement_base_metrics}, {sb_campaignPlacement_addtl_metrics}'
                }
            },
            "sbTargeting": {
                "['targeting']": {
                        "table_name": "targeting",
                        "metrics": sb_targeting_base_metrics
                }
            },
            "sbSearchTerm": {
                "['searchTerm']": {
                        "table_name": "search_term",
                        "metrics": f"{sb_searchTerm_base_metrics}, {sb_searchTerm_addtl_metrics}"
                }
            },
            "sbAds": {
                "['ads']": {
                    "table_name": "ad",
                    "metrics": sb_ad_base_metrics
                }
            },
            "sbPurchasedProduct": {
                "['purchasedAsin']": {
                    "table_name": "purchased_product",
                    "metrics": sb_purchasedProduct_base_metrics
                }
            },
            "sbGrossAndInvalids": {
                "['campaign']": {
                    "table_name": "gross_and_invalid",
                    "metrics": gross_and_invalid_metrics
                }
            }
        },
        "SPONSORED_DISPLAY": {
            "sdCampaigns": {
                "['campaign']": {
                        "table_name": "campaign",
                        "metrics": f'{sd_campaign_base_metrics}, {sd_campaign_addtl_metrics}'
                },
                "['campaign', 'matchedTarget']": {
                        "table_name": "matched_target",
                        "metrics": f'{sd_campaign_base_metrics}, {"campaignBudgetAmount, campaignStatus, costType"}, {sd_matchedTarget_addtl_metrics}'
                },
            },
            "sdAdGroup": {
                "['adGroup']": {
                        "table_name": "adgroup",
                        "metrics": f'{sd_adgroup_base_metrics}, {sd_adgroup_addtl_metrics}'
                },
                "['adGroup', 'matchedTarget']": {
                        "table_name": "matched_target",
                        "metrics": f'{sd_adgroup_base_metrics}, {sd_matchedTarget_addtl_metrics}'
                },
            },
            "sdTargeting": {
                "['targeting']": {
                    "table_name": "targeting",
                    "metrics": f'{sd_targeting_base_metrics}, {sd_targeting_addtl_metrics}'
                },
                "['targeting', 'matchedTarget']": {
                    "table_name": "targeting_matched_asin",
                    "metrics": f'{sd_targeting_base_metrics}, {"adKeywordStatus"}, {sd_matchedTarget_addtl_metrics}'
                }
            },
            "sdAdvertisedProduct": {
                "['advertiser']": {
                    "table_name": "advertised_product",
                    "metrics": sd_advertisedProduct_base_metrics
                }
            },
            "sdGrossAndInvalids": {
                "['campaign']": {
                    "table_name": "gross_and_invalid",
                    "metrics": gross_and_invalid_metrics
                }
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
        "['asin']": sp_purchasedProduct_base_metrics
    },
    # Sponsored Brands Video (version 3)
    "SPONSORED_BRANDS": {
        "['campaign']": sb_campaign_base_metrics, # Additional metrics already in base metrics
        "['adGroup']": sb_adgroup_base_metrics,
        # "['campaign']": 


        "['purchasedAsin']": sb_purchasedProduct_base_metrics
    },
    "SPONSORED_DISPLAY": {
        "['campaign']": f'{sd_campaign_base_metrics}, {sd_campaign_addtl_metrics}',
        "['campaign', 'matchedTarget']": f'{sd_campaign_base_metrics}, {sd_campaign_addtl_metrics}, {sd_matchedTarget_addtl_metrics}',
        "['adGroup']": f'{sd_adgroup_base_metrics}, {sd_adgroup_addtl_metrics}',
        "['adGroup', 'matchedTarget']": f'{sd_adgroup_base_metrics}, {sd_adgroup_addtl_metrics}, {sd_matchedTarget_addtl_metrics}'
    }
}


filters = {
    'spSearchTerm': ['TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED'],
    'spPurchasedProduct': ['BROAD', 'PHRASE', 'EXACT', 'TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED']
}