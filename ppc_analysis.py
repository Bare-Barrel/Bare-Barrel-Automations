import postgresql
import pandas as pd
import numpy as np
import datetime as dt

def get_data(country):
    query = f"""
            CREATE TEMP VIEW search_query_performance_latest
                AS SELECT t2.category, "Search Query" search_query, MAX("Search Query Volume") search_query_volume,
                    MAX("Purchases: Total Count") total_purchases, SUM("Impressions: ASIN Count") impressions,
                    SUM("Clicks: ASIN Count") clicks, SUM("Purchases: ASIN Count") purchases
                    FROM search_query_performance t1
                    LEFT JOIN product_amazon t2
                        ON t1.asin = t2.asin
                    WHERE t1."Reporting Date" = (SELECT MAX("Reporting Date") FROM search_query_performance) AND t1.country = '{country}'
                    GROUP BY t2.category, t1."Search Query";

            CREATE TEMP VIEW cerebro_latest
                AS SELECT category, "Keyword Phrase" keyword_phrase, "Search Volume" cerebro_search_volume, "Position (Rank)" position_rank, "Relative Rank" relative_rank, 
                    "Competitor Rank (avg)" competitor_rank_avg, "Ranking Competitors (count)" ranking_competitors_count, "Competitor Performance Score" competitor_performance_score,
                    "H10 PPC Sugg. Bid" h10_sugg_bid, "H10 PPC Sugg. Min Bid" h10_min_bid, "H10 PPC Sugg. Max Bid" h10_max_bid
                    FROM cerebro_amazon t1
                    WHERE created::DATE = (SELECT MAX(created)::DATE FROM cerebro_amazon) AND country = '{country}'
                    AND "Competitor Performance Score" > 3;

            SELECT t1.category, t2.category category2, COALESCE(t1.search_query, t2.keyword_phrase) keyword_phrase, t1.search_query_volume, t2.cerebro_search_volume,
                t1.total_purchases, t1.impressions, t1.impressions/t1.search_query_volume impression_share,
                t1.clicks, t1.clicks::FLOAT/t1.impressions::FLOAT click_through_rate, t1.purchases, 
                COALESCE(t1.purchases::FLOAT/NULLIF(t1.clicks::FLOAT,0), 0) conversion_rate,
                t2.position_rank, t2.relative_rank, t2.competitor_rank_avg, t2.ranking_competitors_count, t2.competitor_performance_score,
                t2.h10_sugg_bid, t2.h10_min_bid, t2.h10_max_bid
                FROM search_query_performance_latest t1
                FULL OUTER JOIN cerebro_latest t2 
                    ON t1.category = t2.category AND t1.search_query = t2.keyword_phrase 
                ORDER BY t1.category ASC, t2.competitor_performance_score DESC, t1.search_query_volume DESC;"""

    data = postgresql.sql_to_dataframe(query)
    print(data)
    # merging ppc data
    ppc_query = """SELECT SUM(impressions), FROM """


    ppc = pd.read_excel('bulk-operations-1-17-2023.xlsx', sheet_name='Sponsored Products Campaigns')
    cols = ['Portfolio Name (Informational only)', 'Keyword Text', 'Match Type', 'Bid', 'CPC', 'Impressions', 'Clicks', 'Orders', 'Spend', 'Sales', 'Conversion Rate', 'Acos']
    ppc = ppc[ppc.Entity == 'Keyword'][cols]
    merged_data = pd.merge(data, ppc, how='outer', left_on=['category', 'keyword_phrase'], right_on=['Portfolio Name (Informational only)', 'Keyword Text'])
    merged_data.to_excel('analysis testing.xlsx', index=False)

if __name__ == '__main__':
    get_data('CA')
