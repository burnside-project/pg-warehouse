-- ============================================================================
-- Layer:       silver
-- Target:      promotion_usage
-- Description: Promotion and coupon usage combining promotion definitions with
--              redemption metrics and order impact.
-- Sources:     promotions, coupon_redemptions, orders
-- ============================================================================

CREATE OR REPLACE TABLE promotion_usage AS
SELECT
    pr.id                                           AS promotion_id,
    pr.code                                         AS promo_code,
    pr.promo_type,
    pr.value                                        AS promo_value,
    pr.min_order,
    pr.max_uses,
    pr.uses                                         AS current_uses,
    pr.starts_at,
    pr.ends_at,

    -- Redemption metrics
    COALESCE(rd.redemption_count, 0)                AS redemption_count,
    COALESCE(rd.total_discount_given, 0)            AS total_discount_given,
    rd.avg_discount_per_use,
    rd.first_redeemed_at,
    rd.last_redeemed_at,

    -- Order impact
    COALESCE(rd.avg_order_total, 0)                 AS avg_order_total_with_coupon,
    COALESCE(rd.unique_customers, 0)                AS unique_customers,

    -- Derived: promotion status
    CASE
        WHEN pr.ends_at IS NOT NULL AND pr.ends_at < CURRENT_TIMESTAMP THEN 'expired'
        WHEN pr.max_uses IS NOT NULL AND pr.uses >= pr.max_uses         THEN 'exhausted'
        WHEN pr.starts_at > CURRENT_TIMESTAMP                           THEN 'scheduled'
        ELSE                                                                 'active'
    END                                             AS promo_status

FROM promotions pr

LEFT JOIN (
    SELECT
        cr.promotion_id,
        COUNT(*)                                    AS redemption_count,
        SUM(cr.discount_amount)                     AS total_discount_given,
        ROUND(AVG(cr.discount_amount), 2)           AS avg_discount_per_use,
        MIN(cr.redeemed_at)                         AS first_redeemed_at,
        MAX(cr.redeemed_at)                         AS last_redeemed_at,
        ROUND(AVG(o.total), 2)                      AS avg_order_total,
        COUNT(DISTINCT cr.customer_id)              AS unique_customers
    FROM coupon_redemptions cr
    LEFT JOIN orders o ON cr.order_id = o.id
    GROUP BY cr.promotion_id
) rd ON pr.id = rd.promotion_id;
