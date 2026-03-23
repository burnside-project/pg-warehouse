-- name: product_catalog
-- materialized: table
-- tags: silver,products

CREATE OR REPLACE TABLE product_catalog AS
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.base_price,
    p.status AS product_status,
    cat.name AS category_name,
    COALESCE(v.variant_count, 0) AS variant_count,
    COALESCE(inv.total_available, 0) AS total_available,
    COALESCE(r.review_count, 0) AS review_count,
    r.avg_rating,
    CASE
        WHEN COALESCE(inv.total_available, 0) = 0 THEN 'out_of_stock'
        WHEN inv.total_available < 10 THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status
FROM {{ source('silver', 'products') }} p
LEFT JOIN {{ source('silver', 'categories') }} cat ON p.category_id = cat.id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS variant_count
    FROM {{ source('silver', 'product_variants') }}
    GROUP BY product_id
) v ON p.id = v.product_id
LEFT JOIN (
    SELECT pv.product_id, SUM(i.qty_available) AS total_available
    FROM {{ source('silver', 'inventory') }} i
    JOIN {{ source('silver', 'product_variants') }} pv ON i.variant_id = pv.id
    GROUP BY pv.product_id
) inv ON p.id = inv.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS review_count, ROUND(AVG(rating), 2) AS avg_rating
    FROM {{ source('silver', 'reviews') }}
    GROUP BY product_id
) r ON p.id = r.product_id;
