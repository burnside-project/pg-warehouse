CREATE OR REPLACE TABLE feat.customer_features AS
SELECT
    c.id AS customer_id,
    c.country,
    COUNT(o.id) AS total_orders,
    COALESCE(SUM(o.amount), 0) AS total_revenue
FROM raw.customers c
LEFT JOIN raw.orders o
  ON c.id = o.customer_id
GROUP BY 1, 2;
