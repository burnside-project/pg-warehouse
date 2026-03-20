-- ============================================================================
-- Layer:       feat
-- Target:      feat.inventory_health
-- Description: Inventory status by product and category — stock levels,
--              sell-through rates, reorder signals. Powers the Operations
--              dashboard.
-- Sources:     silver.product_catalog, silver.product_sales
-- ============================================================================

CREATE OR REPLACE TABLE feat.inventory_health AS
SELECT
    pc.product_id,
    pc.product_name,
    pc.category_name,
    pc.stock_status,

    -- Current inventory
    pc.total_available,
    pc.total_reserved,
    pc.total_available - pc.total_reserved          AS net_available,

    -- Recent velocity
    COALESCE(ps.units_sold_30d, 0)                  AS units_sold_last_30d,

    -- Days of stock remaining (at current 30d velocity)
    CASE
        WHEN COALESCE(ps.units_sold_30d, 0) > 0
        THEN ROUND((pc.total_available - pc.total_reserved) * 30.0 / ps.units_sold_30d, 1)
        ELSE NULL
    END                                             AS estimated_days_of_stock,

    -- Reorder signal
    CASE
        WHEN pc.total_available = 0                                      THEN 'reorder_urgent'
        WHEN pc.total_available < 10                                     THEN 'reorder_soon'
        WHEN COALESCE(ps.units_sold_30d, 0) > 0
             AND (pc.total_available - pc.total_reserved) * 30.0 / ps.units_sold_30d < 14
                                                                         THEN 'reorder_soon'
        ELSE                                                                  'healthy'
    END                                             AS reorder_signal,

    -- Variant count and pricing
    pc.variant_count,
    pc.base_price,
    pc.review_count,
    pc.avg_rating

FROM silver.product_catalog pc
LEFT JOIN silver.product_sales ps ON pc.product_id = ps.product_id
WHERE pc.product_status = 'active'
ORDER BY
    CASE
        WHEN pc.total_available = 0 THEN 0
        WHEN pc.total_available < 10 THEN 1
        ELSE 2
    END,
    COALESCE(ps.units_sold_30d, 0) DESC;
