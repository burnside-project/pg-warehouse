-- ============================================================================
-- Layer:       silver
-- Target:      silver.product_catalog
-- Description: Complete product catalog combining products with variants,
--              categories, and aggregated review metrics.
-- Sources:     raw.products, raw.product_variants, raw.categories, raw.reviews
-- ============================================================================

CREATE OR REPLACE TABLE silver.product_catalog AS
SELECT
    p.id                                            AS product_id,
    p.name                                          AS product_name,
    p.description,
    p.sku,
    p.created_at                                    AS listed_at,
    p.updated_at                                    AS product_updated_at,

    -- Category
    cat.name                                        AS category_name,
    cat.parent_id                                   AS parent_category_id,

    -- Pricing (from variants)
    v.variant_count,
    v.min_price,
    v.max_price,
    v.total_inventory,

    -- Review metrics
    COALESCE(r.review_count, 0)                     AS review_count,
    r.avg_rating,
    r.min_rating,
    r.max_rating,

    -- Derived
    CASE
        WHEN COALESCE(v.total_inventory, 0) = 0 THEN 'out_of_stock'
        WHEN v.total_inventory < 10                  THEN 'low_stock'
        ELSE                                              'in_stock'
    END                                             AS stock_status

FROM raw.products p
LEFT JOIN raw.categories cat
    ON p.category_id = cat.id
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*)                    AS variant_count,
        MIN(price)                  AS min_price,
        MAX(price)                  AS max_price,
        SUM(inventory_quantity)     AS total_inventory
    FROM raw.product_variants
    GROUP BY product_id
) v ON p.id = v.product_id
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*)                    AS review_count,
        ROUND(AVG(rating), 2)      AS avg_rating,
        MIN(rating)                 AS min_rating,
        MAX(rating)                 AS max_rating
    FROM raw.reviews
    GROUP BY product_id
) r ON p.id = r.product_id;
