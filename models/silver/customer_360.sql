-- name: customer_360
-- materialized: table
-- tags: silver,customers

CREATE OR REPLACE TABLE customer_360 AS
SELECT
    c.id AS customer_id,
    c.email,
    c.name AS customer_name,
    c.created_at AS customer_since,
    COALESCE(om.total_orders, 0) AS total_orders,
    COALESCE(om.lifetime_revenue, 0) AS lifetime_revenue,
    COALESCE(om.avg_order_value, 0) AS avg_order_value,
    om.first_order_date,
    om.last_order_date,
    CASE
        WHEN om.total_orders IS NULL THEN 'never_purchased'
        WHEN om.total_orders = 1 THEN 'one_time'
        WHEN om.total_orders <= 3 THEN 'occasional'
        WHEN om.total_orders <= 10 THEN 'regular'
        ELSE 'loyal'
    END AS customer_segment
FROM {{ source('silver', 'customers') }} c
LEFT JOIN (
    SELECT customer_id,
        COUNT(DISTINCT id) AS total_orders,
        SUM(total) AS lifetime_revenue,
        ROUND(AVG(total), 2) AS avg_order_value,
        MIN(placed_at) AS first_order_date,
        MAX(placed_at) AS last_order_date
    FROM {{ source('silver', 'orders') }}
    GROUP BY customer_id
) om ON c.id = om.customer_id;
