-- ============================================================================
-- Layer:       feat
-- Target:      feat.product_performance
-- Description: Product rankings by revenue, volume, rating, and inventory
--              health. Powers the Product dashboard.
-- Sources:     silver.product_catalog, silver.product_sales
-- ============================================================================

CREATE OR REPLACE TABLE feat.product_performance AS
SELECT
    pc.product_id,
    pc.product_name,
    pc.slug,
    pc.category_name,
    pc.product_status,
    pc.listed_at,

    -- Pricing
    pc.base_price,
    pc.min_price,
    pc.max_price,
    pc.variant_count,

    -- Sales
    COALESCE(ps.orders_containing_product, 0)        AS total_orders,
    COALESCE(ps.units_sold, 0)                       AS units_sold,
    COALESCE(ps.product_revenue, 0)                  AS total_revenue,

    -- Reviews
    pc.review_count,
    pc.avg_rating,

    -- Inventory
    pc.total_available,
    pc.total_reserved,
    pc.stock_status,

    -- Price volatility
    pc.price_change_count,
    pc.last_price_change_at,

    -- Rankings
    RANK() OVER (ORDER BY COALESCE(ps.product_revenue, 0) DESC)  AS revenue_rank,
    RANK() OVER (ORDER BY COALESCE(ps.units_sold, 0) DESC)       AS volume_rank,
    RANK() OVER (ORDER BY COALESCE(pc.avg_rating, 0) DESC)       AS rating_rank

FROM silver.product_catalog pc
LEFT JOIN silver.product_sales ps
    ON pc.product_id = ps.product_id
ORDER BY total_revenue DESC;
