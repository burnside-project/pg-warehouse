-- ============================================================================
-- Layer:       feat
-- Target:      feat.promotion_effectiveness
-- Description: Promotion ROI analysis — redemption rates, discount impact,
--              customer reach, and status tracking. Powers the Marketing
--              dashboard.
-- Sources:     silver.promotion_usage
-- ============================================================================

CREATE OR REPLACE TABLE feat.promotion_effectiveness AS
SELECT
    promotion_id,
    promo_code,
    promo_type,
    promo_value,
    min_order,
    promo_status,
    starts_at,
    ends_at,

    -- Usage
    max_uses,
    current_uses,
    redemption_count,
    unique_customers,

    -- Financials
    total_discount_given,
    avg_discount_per_use,
    avg_order_total_with_coupon,

    -- Utilization rate
    CASE
        WHEN max_uses IS NOT NULL AND max_uses > 0
        THEN ROUND(current_uses * 100.0 / max_uses, 2)
        ELSE NULL
    END                                             AS utilization_pct,

    -- Timing
    first_redeemed_at,
    last_redeemed_at,
    DATE_DIFF('day', starts_at, COALESCE(ends_at, CURRENT_TIMESTAMP))
                                                    AS campaign_duration_days,

    -- Derived: effectiveness tier
    CASE
        WHEN redemption_count = 0                    THEN 'unused'
        WHEN unique_customers >= 100                 THEN 'high_reach'
        WHEN unique_customers >= 20                  THEN 'medium_reach'
        ELSE                                              'low_reach'
    END                                             AS reach_tier

FROM silver.promotion_usage
ORDER BY total_discount_given DESC;
