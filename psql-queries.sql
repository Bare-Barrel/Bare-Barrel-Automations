SELECT DISTINCT(reporting_date) FROM search_query_performance_asin_view
WHERE country = 'CA'
ORDER BY reporting_date;

UPDATE search_query_performance_brand_view 
SET search_query = 'cocktail set(?)'
WHERE search_query = 'cocktail set?' AND country = 'CA';

SELECT reporting_date, search_query, country FROM search_query_performance_brand_view
WHERE search_query LIKE '%?%';

SELECT reporting_date, search_query, SUM(search_query_volume), SUM(purchases_total_count), SUM(purchases_brand_count)
FROM search_query_performance_brand_view
GROUP BY reporting_date, search_query
ORDER BY AVG(search_query_score) ASC;


CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT * FROM crosstab('SELECT search_query, reporting_date, search_query_volume
					   		FROM search_query_performance_brand_view
						 	ORDER BY 1, 2',
					  'SELECT DISTINCT reporting_date FROM search_query_performance_brand_view ORDER BY 1') 
							AS ct (search_query text, "2023-05-13" date, "2023-05-20" date);
							
							
SELECT MAX(reporting_date) FROM search_query_performance_brand_view;


SELECT search_query, reporting_date, search_query_volume, impressions_total_count, purchases_total_count, purchases_brand_count 
					   		FROM search_query_performance_brand_view
							WHERE search_query IN ('bartender kit', 'mixology bartender kit', 'mixology kit', 'cocktail shaker set', 'cocktail kit')
							AND reporting_date > '2023-04-01'
							AND country = 'US'
							ORDER BY reporting_date, search_query;
	