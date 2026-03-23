-- name: product_performance
-- materialized: parquet
-- tags: features,products

CREATE OR REPLACE TABLE product_performance AS
SELECT
    pc.product_id,
    pc.product_name,
    pc.category_name,
    pc.base_price,
    pc.stock_status,
    pc.review_count,
    pc.avg_rating,
    pc.total_available,
    RANK() OVER (ORDER BY COALESCE(pc.avg_rating, 0) DESC) AS rating_rank
FROM {{ ref('product_catalog') }} pc
ORDER BY rating_rank;
