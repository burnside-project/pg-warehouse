-- ============================================================================
-- Layer:       silver
-- Target:      silver.customer_360
-- Description: Unified customer profile combining demographics with lifetime
--              order metrics, first/last purchase dates, and address info.
-- Sources:     raw.customers, raw.orders, raw.order_items, raw.addresses
-- ============================================================================

CREATE OR REPLACE TABLE silver.customer_360 AS
SELECT
    c.id                                            AS customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name              AS full_name,
    c.phone,
    c.created_at                                    AS customer_since,

    -- Address (primary/latest)
    a.city,
    a.state,
    a.country,
    a.postal_code,

    -- Lifetime order metrics
    COALESCE(om.total_orders, 0)                    AS total_orders,
    COALESCE(om.total_items, 0)                     AS total_items,
    COALESCE(om.lifetime_revenue, 0)                AS lifetime_revenue,
    COALESCE(om.avg_order_value, 0)                 AS avg_order_value,
    om.first_order_date,
    om.last_order_date,

    -- Derived
    CASE
        WHEN om.total_orders IS NULL THEN 'never_purchased'
        WHEN om.total_orders = 1    THEN 'one_time'
        WHEN om.total_orders <= 3   THEN 'occasional'
        ELSE                             'loyal'
    END                                             AS customer_segment,

    DATE_DIFF('day', om.first_order_date, om.last_order_date)
                                                    AS customer_lifetime_days

FROM raw.customers c
LEFT JOIN (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        city,
        state,
        country,
        postal_code
    FROM raw.addresses
    ORDER BY customer_id, is_primary DESC, created_at DESC
) a ON c.id = a.customer_id
LEFT JOIN (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.id)                        AS total_orders,
        COALESCE(SUM(oi.quantity), 0)               AS total_items,
        COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS lifetime_revenue,
        COALESCE(AVG(sub.order_total), 0)           AS avg_order_value,
        MIN(o.created_at)                           AS first_order_date,
        MAX(o.created_at)                           AS last_order_date
    FROM raw.orders o
    LEFT JOIN raw.order_items oi ON o.id = oi.order_id
    LEFT JOIN (
        SELECT order_id, SUM(quantity * unit_price) AS order_total
        FROM raw.order_items
        GROUP BY order_id
    ) sub ON o.id = sub.order_id
    GROUP BY o.customer_id
) om ON c.id = om.customer_id;
