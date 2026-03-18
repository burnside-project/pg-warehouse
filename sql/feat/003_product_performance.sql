-- ============================================================================
-- Layer:       feat
-- Target:      feat.product_performance
-- Description: Product rankings by revenue, review scores, and inventory
--              status. Powers the Product dashboard.
-- Sources:     silver.product_catalog, silver.order_enriched
-- ============================================================================

CREATE OR REPLACE TABLE feat.product_performance AS
WITH product_sales AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oe.order_id)                 AS orders_containing_product,
        SUM(oi.quantity)                             AS units_sold,
        SUM(oi.quantity * oi.unit_price)             AS product_revenue,
        SUM(oi.discount_amount)                     AS product_discounts
    FROM raw.order_items oi
    INNER JOIN silver.order_enriched oe
        ON oi.order_id = oe.order_id
    GROUP BY oi.product_id
)
SELECT
    pc.product_id,
    pc.product_name,
    pc.sku,
    pc.category_name,
    pc.listed_at,

    -- Pricing
    pc.min_price,
    pc.max_price,
    pc.variant_count,

    -- Sales
    COALESCE(ps.orders_containing_product, 0)       AS total_orders,
    COALESCE(ps.units_sold, 0)                      AS units_sold,
    COALESCE(ps.product_revenue, 0)                 AS total_revenue,
    COALESCE(ps.product_discounts, 0)               AS total_discounts,

    -- Reviews
    pc.review_count,
    pc.avg_rating,

    -- Inventory
    pc.total_inventory,
    pc.stock_status,

    -- Rankings
    RANK() OVER (ORDER BY COALESCE(ps.product_revenue, 0) DESC)  AS revenue_rank,
    RANK() OVER (ORDER BY COALESCE(ps.units_sold, 0) DESC)       AS volume_rank,
    RANK() OVER (ORDER BY COALESCE(pc.avg_rating, 0) DESC)       AS rating_rank

FROM silver.product_catalog pc
LEFT JOIN product_sales ps
    ON pc.product_id = ps.product_id
ORDER BY total_revenue DESC;
