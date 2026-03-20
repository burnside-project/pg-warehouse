-- ============================================================================
-- Layer:       silver
-- Target:      silver.customer_360
-- Description: Unified customer profile combining demographics with lifetime
--              order metrics, default shipping address, and review activity.
-- Sources:     raw.customers, raw.orders, raw.order_items, raw.addresses,
--              raw.reviews
-- ============================================================================

CREATE OR REPLACE TABLE silver.customer_360 AS
SELECT
    c.id                                            AS customer_id,
    c.email,
    c.name                                          AS customer_name,
    c.last_login,
    c.created_at                                    AS customer_since,

    -- Default shipping address
    a.city,
    a.state,
    a.zip,
    a.country,

    -- Lifetime order metrics
    COALESCE(om.total_orders, 0)                    AS total_orders,
    COALESCE(om.total_items, 0)                     AS total_items,
    COALESCE(om.lifetime_revenue, 0)                AS lifetime_revenue,
    COALESCE(om.avg_order_value, 0)                 AS avg_order_value,
    om.first_order_date,
    om.last_order_date,

    -- Review activity
    COALESCE(rv.review_count, 0)                    AS review_count,
    rv.avg_rating_given,

    -- Derived: customer segment
    CASE
        WHEN om.total_orders IS NULL THEN 'never_purchased'
        WHEN om.total_orders = 1     THEN 'one_time'
        WHEN om.total_orders <= 3    THEN 'occasional'
        WHEN om.total_orders <= 10   THEN 'regular'
        ELSE                              'loyal'
    END                                             AS customer_segment,

    -- Derived: customer lifetime days
    DATE_DIFF('day', om.first_order_date, om.last_order_date)
                                                    AS customer_lifetime_days

FROM raw.customers c

-- Default address (is_default=true, fallback to latest)
LEFT JOIN (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        city,
        state,
        zip,
        country
    FROM raw.addresses
    WHERE addr_type = 'shipping'
    ORDER BY customer_id, is_default DESC, created_at DESC
) a ON c.id = a.customer_id

-- Order aggregates
LEFT JOIN (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.id)                        AS total_orders,
        COALESCE(SUM(oi.qty), 0)                    AS total_items,
        SUM(o.total)                                AS lifetime_revenue,
        ROUND(AVG(o.total), 2)                      AS avg_order_value,
        MIN(o.placed_at)                            AS first_order_date,
        MAX(o.placed_at)                            AS last_order_date
    FROM raw.orders o
    LEFT JOIN raw.order_items oi ON o.id = oi.order_id
    GROUP BY o.customer_id
) om ON c.id = om.customer_id

-- Review aggregates
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)                                    AS review_count,
        ROUND(AVG(rating), 2)                       AS avg_rating_given
    FROM raw.reviews
    GROUP BY customer_id
) rv ON c.id = rv.customer_id;
