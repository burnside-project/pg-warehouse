-- name: customer_analytics
-- materialized: parquet
-- tags: features,customers

CREATE OR REPLACE TABLE customer_analytics AS
SELECT
    customer_id,
    customer_name,
    customer_segment,
    total_orders,
    lifetime_revenue,
    avg_order_value,
    CASE
        WHEN last_order_date IS NULL THEN 'never_active'
        WHEN DATE_DIFF('day', last_order_date::TIMESTAMP, CURRENT_TIMESTAMP::TIMESTAMP) <= 30 THEN 'active'
        WHEN DATE_DIFF('day', last_order_date::TIMESTAMP, CURRENT_TIMESTAMP::TIMESTAMP) <= 90 THEN 'cooling'
        WHEN DATE_DIFF('day', last_order_date::TIMESTAMP, CURRENT_TIMESTAMP::TIMESTAMP) <= 180 THEN 'at_risk'
        ELSE 'churned'
    END AS activity_status
FROM {{ ref('customer_360') }}
ORDER BY lifetime_revenue DESC;
