-- name: sales_summary
-- materialized: parquet
-- tags: features,sales

CREATE OR REPLACE TABLE sales_summary AS
SELECT
    CAST(order_date::TIMESTAMP AS DATE) AS sale_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(total_quantity) AS units_sold,
    SUM(order_total) AS gross_revenue,
    SUM(coupon_discount) AS total_discounts,
    ROUND(AVG(order_total), 2) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN shipment_status = 'delivered' THEN order_id END) AS delivered_orders
FROM {{ ref('order_enriched') }}
GROUP BY CAST(order_date::TIMESTAMP AS DATE)
ORDER BY sale_date;
