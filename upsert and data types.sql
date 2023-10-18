CREATE TEMP TABLE tmp_table
ON COMMIT DROP
AS
SELECT * 
FROM sponsored_product_search_term_report
WITH NO DATA;

COPY tmp_table FROM '/Users/calvin/Google Drive/Shared drives/BB: Shared Drive/Calvin - Personal Folder/PPC-Automation/PPC Data/Sponsored Products Search term report (1).xlsx';

INSERT INTO sponsored_product_search_term_report
SELECT DISTINCT ON (date, campaign_name, ad_group_name, targeting, customer_search_term) *
FROM tmp_table;



BEGIN;
CREATE TEMP TABLE tmp_table 
(LIKE sponsored_product_search_term_report INCLUDING DEFAULTS)
ON COMMIT DROP;
    
\copy tmp_table FROM '/Users/calvin/Google Drive/Shared drives/BB: Shared Drive/Calvin - Personal Folder/PPC-Automation/PPC Data/Sponsored Products Search term report (1).xlsx'
CSV HEADER;
    
INSERT INTO main_table
SELECT *
FROM tmp_table
ON CONFLICT DO NOTHING;
COMMIT;



SELECT * FROM sponsored_product_search_term_report LIMIT 0;


CREATE TEMPORARY TABLE temp_table (LIKE sponsored_product_search_term_report);
SELECT * FROM temp_table;


select * from pg_type;


SELECT attname
     , atttypid::regtype AS base_type
     , format_type(atttypid, atttypmod) AS full_type
FROM   pg_catalog.pg_attribute
WHERE  attrelid = 'search_query_performance_asin_view'::regclass  -- your table name here
AND    attnum > 0
AND    NOT attisdropped;


SELECT atttypid::regtype base_type, atttypid, atttypmod FROM pg_catalog.pg_attribute;
ORDER BY full_type;