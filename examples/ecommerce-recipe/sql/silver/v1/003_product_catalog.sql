-- ============================================================================
-- Layer:       silver
-- Target:      product_catalog
-- Description: Complete product catalog combining products with variants,
--              categories, inventory levels, price history, and review metrics.
-- Sources:     products, product_variants, categories,
--              inventory, price_history, reviews
-- ============================================================================

CREATE OR REPLACE TABLE product_catalog AS
SELECT
    p.id                                            AS product_id,
    p.name                                          AS product_name,
    p.slug,
    p.description,
    p.base_price,
    p.status                                        AS product_status,
    p.created_at                                    AS listed_at,

    -- Category
    cat.name                                        AS category_name,
    cat.slug                                        AS category_slug,
    cat.parent_id                                   AS parent_category_id,

    -- Variant aggregates
    COALESCE(v.variant_count, 0)                    AS variant_count,
    v.min_price,
    v.max_price,
    v.avg_weight_grams,

    -- Inventory (across all variants and warehouses)
    COALESCE(inv.total_available, 0)                AS total_available,
    COALESCE(inv.total_reserved, 0)                 AS total_reserved,

    -- Price changes
    COALESCE(ph.price_change_count, 0)              AS price_change_count,
    ph.last_price_change_at,

    -- Review metrics
    COALESCE(r.review_count, 0)                     AS review_count,
    r.avg_rating,
    r.min_rating,
    r.max_rating,

    -- Derived: stock status
    CASE
        WHEN COALESCE(inv.total_available, 0) = 0 THEN 'out_of_stock'
        WHEN inv.total_available < 10               THEN 'low_stock'
        ELSE                                             'in_stock'
    END                                             AS stock_status

FROM products p

LEFT JOIN categories cat
    ON p.category_id = cat.id

-- Variant pricing
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*)                                    AS variant_count,
        MIN(COALESCE(price_override, 0))            AS min_price,
        MAX(COALESCE(price_override, 0))            AS max_price,
        ROUND(AVG(weight_grams), 0)                 AS avg_weight_grams
    FROM product_variants
    GROUP BY product_id
) v ON p.id = v.product_id

-- Inventory totals (join variants → inventory)
LEFT JOIN (
    SELECT
        pv.product_id,
        SUM(i.qty_available)                        AS total_available,
        SUM(i.qty_reserved)                         AS total_reserved
    FROM inventory i
    JOIN product_variants pv ON i.variant_id = pv.id
    GROUP BY pv.product_id
) inv ON p.id = inv.product_id

-- Price history
LEFT JOIN (
    SELECT
        pv.product_id,
        COUNT(*)                                    AS price_change_count,
        MAX(ph.changed_at)                          AS last_price_change_at
    FROM price_history ph
    JOIN product_variants pv ON ph.variant_id = pv.id
    GROUP BY pv.product_id
) ph ON p.id = ph.product_id

-- Reviews
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*)                                    AS review_count,
        ROUND(AVG(rating), 2)                       AS avg_rating,
        MIN(rating)                                 AS min_rating,
        MAX(rating)                                 AS max_rating
    FROM reviews
    GROUP BY product_id
) r ON p.id = r.product_id;
