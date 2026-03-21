-- ============================================================================
-- Layer:       silver
-- Target:      product_sales
-- Description: Per-product sales aggregates combining order items with variant
--              mappings. Includes all-time totals and trailing-30-day velocity.
-- Sources:     order_items, product_variants, orders
-- ============================================================================

CREATE OR REPLACE TABLE product_sales AS
SELECT
    pv.product_id,

    -- All-time totals
    COUNT(DISTINCT oi.order_id)                 AS orders_containing_product,
    SUM(oi.qty)                                 AS units_sold,
    SUM(oi.line_total)                          AS product_revenue,

    -- Trailing 30-day velocity
    SUM(CASE WHEN o.placed_at >= CURRENT_DATE - INTERVAL '30 days'
             THEN oi.qty ELSE 0
        END)                                    AS units_sold_30d

FROM order_items oi
JOIN product_variants pv ON oi.variant_id = pv.id
JOIN orders o ON oi.order_id = o.id
GROUP BY pv.product_id;