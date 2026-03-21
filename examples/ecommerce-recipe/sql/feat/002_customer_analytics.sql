-- ============================================================================
-- Layer:       feat
-- Target:      customer_analytics
-- Description: Customer cohort analysis, LTV estimates, geographic breakdown,
--              segmentation, and activity status. Powers the Customer dashboard.
-- Sources:     customer_360
-- ============================================================================

CREATE OR REPLACE TABLE customer_analytics AS
SELECT
    customer_id,
    customer_name,
    email,
    customer_since,
    customer_segment,

    -- Geography
    city,
    state,
    country,

    -- Order metrics
    total_orders,
    total_items,
    lifetime_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    customer_lifetime_days,

    -- Reviews
    review_count,
    avg_rating_given,

    -- Cohort (signup month)
    DATE_TRUNC('month', customer_since)             AS signup_cohort,

    -- Recency (days since last order)
    DATE_DIFF('day', last_order_date, CURRENT_DATE) AS days_since_last_order,

    -- LTV estimate (simple: avg monthly revenue * 12)
    CASE
        WHEN customer_lifetime_days > 30 THEN
            ROUND(lifetime_revenue / (customer_lifetime_days / 30.0) * 12, 2)
        ELSE lifetime_revenue
    END                                             AS estimated_annual_ltv,

    -- Activity status
    CASE
        WHEN last_order_date IS NULL                                      THEN 'never_active'
        WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 30       THEN 'active'
        WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 90       THEN 'cooling'
        WHEN DATE_DIFF('day', last_order_date, CURRENT_DATE) <= 180      THEN 'at_risk'
        ELSE                                                                   'churned'
    END                                             AS activity_status

FROM customer_360
ORDER BY lifetime_revenue DESC;
