worksheet_queries = {
    'FBA-Inv+': '''select * from inventory.fba_planning_inventory
                    order by snapshot_date desc, marketplace, sku;''',

    'FBA-Inv': '''select * from inventory.fba
                    order by date desc, marketplace asc, asin asc;''',

    'Bus-RP': '''select date, parent_asin, marketplace, traffic_by_asin_mobile_app_sessions, traffic_by_asin_mobile_app_sessions_b2b, traffic_by_asin_browser_sessions,
                    traffic_by_asin_browser_sessions_b2b, traffic_by_asin_sessions, traffic_by_asin_sessions_b2b, (traffic_by_asin_mobile_app_session_percentage_b2b/100) traffic_by_asin_mobile_app_session_percentage_b2b,
                    (traffic_by_asin_mobile_app_session_percentage/100) traffic_by_asin_mobile_app_session_percentage, (traffic_by_asin_browser_session_percentage/100) traffic_by_asin_browser_session_percentage,
                    (traffic_by_asin_browser_session_percentage_b2b/100) traffic_by_asin_browser_session_percentage_b2b, (traffic_by_asin_session_percentage/100) traffic_by_asin_session_percentage,
                    (traffic_by_asin_session_percentage_b2b/100) traffic_by_asin_session_percentage_b2b, traffic_by_asin_mobile_app_page_views, traffic_by_asin_mobile_app_page_views_b2b,
                    traffic_by_asin_browser_page_views, traffic_by_asin_browser_page_views_b2b, traffic_by_asin_page_views, traffic_by_asin_page_views_b2b, (traffic_by_asin_mobile_app_page_views_percentage/100) traffic_by_asin_mobile_app_page_views_percentage,
                    (traffic_by_asin_mobile_app_page_views_percentage_b2b/100) traffic_by_asin_mobile_app_page_views_percentage_b2b, (traffic_by_asin_browser_page_views_percentage/100) traffic_by_asin_browser_page_views_percentage,
                    (traffic_by_asin_browser_page_views_percentage_b2b/100) traffic_by_asin_browser_page_views_percentage_b2b, (traffic_by_asin_page_views_percentage/100) traffic_by_asin_page_views_percentage,
                    (traffic_by_asin_page_views_percentage_b2b/100) traffic_by_asin_page_views_percentage_b2b, (traffic_by_asin_buy_box_percentage/100) traffic_by_asin_buy_box_percentage,
                    (traffic_by_asin_buy_box_percentage_b2b/100) traffic_by_asin_buy_box_percentage_b2b, sales_by_asin_units_ordered, sales_by_asin_units_ordered_b2b,
                    (traffic_by_asin_unit_session_percentage/100) traffic_by_asin_unit_session_percentage, (traffic_by_asin_unit_session_percentage_b2b/100) traffic_by_asin_unit_session_percentage_b2b,
                    sales_by_asin_ordered_product_sales_amount, sales_by_asin_ordered_product_sales_b2b_amount, sales_by_asin_total_order_items, sales_by_asin_total_order_items_b2b
                from business_reports.detail_page_sales_and_traffic_parent
                order by date asc, marketplace asc, parent_asin;''',

    'SP-US': '''-- API data source. (04-02-2023 to present)
                select
                    t1.date,
                    t1.marketplace,
                    t3.name portfolio_name,
                    case
                        when t3.name like '%-%' then split_part(t3.name, '-', 1)
                    else null
                    end as product_code,
                    t1.campaign_budget_currency_code currency,
                    sum(t1.cost) total_cost,
                    sum(t1.units_sold_clicks_7d) total_units_sold_clicks_7d,
                    sum(t1.clicks) total_clicks,
                    sum(t1.impressions) total_impressions,
                    sum(t1.sales_7d) sales_7d
                from sponsored_products.campaign t1
                left join (select distinct on (campaign_id)
                            campaign_id,
                            portfolio_id
                        from sponsored_products.targeting
                        order by campaign_id, date desc) as t2
                    on t1.campaign_id = t2.campaign_id
                left join amazon_advertising_portfolios t3
                    on t2.portfolio_id = t3.portfolio_id
                where t1.marketplace = 'US'
                group by t1.marketplace, portfolio_name, t1.date, currency
                having sum(t1.impressions) > 0

                UNION

                -- Advertising reports data source. (06-27-2022 to 2024-04-01)
                select
                    date,
                    marketplace,
                    portfolio_name,
                    case
                        when portfolio_name like '%-%' then split_part(portfolio_name, '-', 1)
                    else null
                    end as product_code,
                    currency,
                    sum(spend) total_cost,
                    sum("7_day_total_orders") total_units_sold_clicks_7d,
                    sum(clicks) total_clicks,
                    sum(impressions) total_impressions,
                    sum("7_day_total_sales") sales_7d
                from sponsored_products.campaign_console
                where marketplace = 'US' and date < '04-02-2023'
                group by marketplace, portfolio_name, date, currency
                order by date asc, marketplace desc, portfolio_name;''',

    'SP-CA': '''-- API data source
                select
                    t1.date,
                    t1.marketplace,
                    t3.name portfolio_name,
                    case
                        when t3.name like '%-%' then split_part(t3.name, '-', 1)
                    else null
                    end as product_code,
                    t1.campaign_budget_currency_code currency,
                    sum(t1.cost) total_cost,
                    sum(t1.units_sold_clicks_7d) total_units_sold_clicks_7d,
                    sum(t1.clicks) total_clicks,
                    sum(t1.impressions) total_impressions,
                    sum(t1.sales_7d) sales_7d
                from sponsored_products.campaign t1
                left join (select distinct on (campaign_id)
                            campaign_id,
                            portfolio_id
                        from sponsored_products.targeting
                        order by campaign_id, date desc) as t2
                    on t1.campaign_id = t2.campaign_id
                left join amazon_advertising_portfolios t3
                    on t2.portfolio_id = t3.portfolio_id
                where t1.marketplace = 'CA'
                group by t1.marketplace, portfolio_name, t1.date, currency;''',

    'SP-UK': '''-- API data source
                select
                    t1.date,
                    t1.marketplace,
                    t3.name portfolio_name,
                    case
                        when t3.name like '%-%' then split_part(t3.name, '-', 1)
                    else null
                    end as product_code,
                    t1.campaign_budget_currency_code currency,
                    sum(t1.cost) total_cost,
                    sum(t1.units_sold_clicks_7d) total_units_sold_clicks_7d,
                    sum(t1.clicks) total_clicks,
                    sum(t1.impressions) total_impressions,
                    sum(t1.sales_7d) sales_7d
                from sponsored_products.campaign t1
                left join (select distinct on (campaign_id)
                            campaign_id,
                            portfolio_id
                        from sponsored_products.targeting
                        order by campaign_id, date desc) as t2
                    on t1.campaign_id = t2.campaign_id
                left join amazon_advertising_portfolios t3
                    on t2.portfolio_id = t3.portfolio_id
                where t1.marketplace = 'UK'
                group by t1.marketplace, portfolio_name, t1.date, currency;''',

    'SB-US': '''-- API Data source v2 (2023-06-11 - present)
                select t1.date, t1.marketplace, t3.name portfolio_name, 
                case
                    when t3.name like '%-%' then split_part(t3.name, '-', 1)
                    when t3.name = 'ALL' then 'ALL'
                else null
                end as product_code,
                t1.currency,
                sum(t1.cost) total_cost, sum(t1.attributed_units_ordered_new_to_brand_14d) attributed_units_ordered_new_to_brand_14d, sum(t1.clicks) total_clicks, sum(t1.impressions) total_impressions, sum(attributed_sales_14d) attributed_sales_14d
                from sponsored_brands.campaign_v2 t1
                left join sponsored_brands.campaigns t2 on t1.campaign_id = t2.campaign_id
                left join amazon_advertising_portfolios t3 on t2.portfolio_id = t3.portfolio_id
                where t1.campaign_status in ('enabled', 'paused') and t1.marketplace = 'US'
                group by t1.date, t3.name, t1.marketplace, t1.currency

                UNION

                -- Advertising reports from advertising console (2022-07-10 - 2023-06-10)
                select date, marketplace, portfolio_name,
                case
                    when portfolio_name like '%-%' then split_part(portfolio_name, '-', 1)
                    when portfolio_name = 'ALL' then 'ALL'
                else null
                end as product_code,
                currency,
                sum(spend) total_cost, sum("14_day_total_units") attributed_units_ordered_new_to_brand_14d, sum(clicks) total_clicks, sum(impressions) total_impressions, sum("14_day_total_sales") attributed_sales_14d
                from sponsored_brands.campaign_console
                where date <= '2023-06-10' and marketplace = 'US'
                group by date, portfolio_name, marketplace, currency
                order by date asc, marketplace asc, portfolio_name asc;''',

    'SB-CA': '''-- API Data source v2 (2023-06-11 - present)
                select t1.date, t1.marketplace, t3.name portfolio_name, 
                case
                    when t3.name like '%-%' then split_part(t3.name, '-', 1)
                    when t3.name = 'ALL' then 'ALL'
                else null
                end as product_code,
                t1.currency,
                sum(t1.cost) total_cost, sum(t1.attributed_units_ordered_new_to_brand_14d) attributed_units_ordered_new_to_brand_14d, sum(t1.clicks) total_clicks, sum(t1.impressions) total_impressions, sum(attributed_sales_14d) attributed_sales_14d
                from sponsored_brands.campaign_v2 t1
                left join sponsored_brands.campaigns t2 on t1.campaign_id = t2.campaign_id
                left join amazon_advertising_portfolios t3 on t2.portfolio_id = t3.portfolio_id
                where t1.campaign_status in ('enabled', 'paused') and t1.marketplace = 'CA'
                group by t1.date, t3.name, t1.marketplace, t1.currency
                order by t1.date asc, t1.marketplace asc, portfolio_name asc;''',

    'SB-UK': '''-- API Data source v2 (2023-06-11 - present)
                select t1.date, t1.marketplace, t3.name portfolio_name, 
                case
                    when t3.name like '%-%' then split_part(t3.name, '-', 1)
                    when t3.name = 'ALL' then 'ALL'
                else null
                end as product_code,
                t1.currency,
                sum(t1.cost) total_cost, sum(t1.attributed_units_ordered_new_to_brand_14d) attributed_units_ordered_new_to_brand_14d, sum(t1.clicks) total_clicks, sum(t1.impressions) total_impressions, sum(attributed_sales_14d) attributed_sales_14d
                from sponsored_brands.campaign_v2 t1
                left join sponsored_brands.campaigns t2 on t1.campaign_id = t2.campaign_id
                left join amazon_advertising_portfolios t3 on t2.portfolio_id = t3.portfolio_id
                where t1.campaign_status in ('enabled', 'paused') and t1.marketplace = 'UK'
                group by t1.date, t3.name, t1.marketplace, t1.currency
                order by t1.date asc, t1.marketplace asc, portfolio_name asc;''',

    'SD': '''-- API console (2023-06-11 - present)
            select t1.date, t1.marketplace, t3.name portfolio_name, 
            case
                when t3.name like '%-%' then split_part(t3.name, '-', 1)
            else null
            end as product_code,
            t1.currency,
            sum(t1.cost) total_cost, sum(t1.attributed_units_ordered_7d) attributed_units_ordered_7d, sum(t1.clicks) total_clicks, sum(t1.impressions) total_impressions
            from sponsored_display.campaign_v2 t1
            left join sponsored_display.campaigns t2 on t1.campaign_id = t2.campaign_id
            left join amazon_advertising_portfolios t3 on t2.portfolio_id = t3.portfolio_id
            group by t1.date, t3.name, t1.marketplace, t1.currency

            UNION

            -- Advertising reports from console (2023-03-13 - 2023-06-10)
            select date, marketplace, portfolio_name,
            case
                when portfolio_name like '%-%' then split_part(portfolio_name, '-', 1)
            else null
            end as product_code,
            currency,
            sum(spend) total_cost, sum("14_day_total_units") attributed_units_ordered_7d, sum(clicks) total_clicks, sum(impressions) total_impressions
            from sponsored_display.campaign_console
            where date <= '2023-06-10'
            group by date, portfolio_name , marketplace, currency
            order by date asc, portfolio_name asc, marketplace asc;''',

    'Orders': '''select 
                    (t1.purchase_date AT TIME ZONE 'GMT')::DATE "GMT-date", (t1.purchase_date AT TIME ZONE 'GMT')::TIME "GMT-time", t1.purchase_date AT TIME ZONE 'GMT' "GMT-datetime", t1.purchase_date AT TIME ZONE 'PDT' "PDT-datetime", (t1.purchase_date AT TIME ZONE 'PDT')::DATE "PDT-date", (REGEXP_MATCHES(seller_sku, '_SL_(.+)'))[1] "CODE", null "Buyer Name", null "Full Name", null "Gift Message",
                    t1.amazon_order_id, t1.amazon_order_id merchant_order_id, t1.purchase_date, t1.last_update_date,
                    t1.order_status, t1.fulfillment_channel, t1.sales_channel, null order_channel, null url, t1.ship_service_level,
                    t2.title product_name, t2.seller_sku, t2.asin, t1.order_status item_status, t2.quantity_ordered, 
                    CASE
                    WHEN (t2.item_price_currency_code IS NULL OR t2.item_price_currency_code = 'nan') AND t1.is_replacement_order = FALSE
                        THEN t3.product_competitive_pricing_competitive_prices->0->'Price'->'ListingPrice'->>'CurrencyCode'
                        ELSE t2.item_price_currency_code
                    END AS "currency",
                    -- Gets Prime Exclusive Price
                    CASE
                    WHEN t2.item_price_amount IS NULL AND t1.is_replacement_order = FALSE
                        THEN (t3.product_competitive_pricing_competitive_prices->0->'Price'->'ListingPrice'->'Amount')::FLOAT * t2.quantity_ordered
                        ELSE t2.item_price_amount
                    END AS "item_price_amount", 
                    t2.item_tax_amount, t2.shipping_price_amount, t2.shipping_tax_amount, null gift_wrap_price, null gift_wrap_tax, t2.promotion_discount_amount, t2.shipping_discount_amount,
                    t1.shipping_address_city, t1.shipping_address_state_or_region, t1.shipping_address_postal_code, t1.shipping_address_country_code, t2.promotion_ids, t1.is_business_order, null purchase_order_number, null price_designation, null signature_confirmation_recommended,
                    t1.is_replacement_order, 
                    CASE
                    WHEN t1.order_status NOT IN ('Pending', 'Canceled') AND t2.item_price_amount IS NULL AND 
                        t1.is_replacement_order = FALSE then 'Y'
                    ELSE t1.replaced_order_id
                    END AS "Vine / replaced_order_id"  
                    from orders.amazon_orders t1
                    left join orders.amazon_order_items t2 on t1.amazon_order_id = t2.amazon_order_id
                    left join (select DISTINCT ON (asin, marketplace) 
                                asin, marketplace, product_competitive_pricing_competitive_prices
                                from product_pricing.competitive_pricing
                                where customer_type = 'Business' and product_competitive_pricing_competitive_prices != '[]'
                                order by asin, marketplace, date desc) as t3 on t2.asin = t3.asin AND t2.marketplace = t3.marketplace 
                    WHERE t1.purchase_date::DATE > '2024-05-31'
                    order by purchase_date asc;'''
}   