-- name: order_enriched
-- materialized: table
-- tags: silver,orders

CREATE OR REPLACE TABLE order_enriched AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.status AS order_status,
    o.placed_at AS order_date,
    o.subtotal,
    o.tax,
    o.shipping AS shipping_cost,
    o.total AS order_total,
    COALESCE(oi.line_item_count, 0) AS line_item_count,
    COALESCE(oi.total_qty, 0) AS total_quantity,
    p.payment_method,
    p.payment_status,
    s.shipment_status,
    s.shipped_at,
    s.delivered_at,
    COALESCE(cr.coupon_discount, 0) AS coupon_discount
FROM {{ source('silver', 'orders') }} o
LEFT JOIN (
    SELECT order_id, COUNT(*) AS line_item_count, SUM(qty) AS total_qty
    FROM {{ source('silver', 'order_items') }}
    GROUP BY order_id
) oi ON o.id = oi.order_id
LEFT JOIN (
    SELECT DISTINCT ON (order_id) order_id, method AS payment_method, status AS payment_status
    FROM {{ source('silver', 'payments') }}
    ORDER BY order_id, created_at DESC
) p ON o.id = p.order_id
LEFT JOIN (
    SELECT DISTINCT ON (order_id) order_id, status AS shipment_status, shipped_at, delivered_at
    FROM {{ source('silver', 'shipments') }}
    ORDER BY order_id, id DESC
) s ON o.id = s.order_id
LEFT JOIN (
    SELECT order_id, SUM(discount_amount) AS coupon_discount
    FROM {{ source('silver', 'coupon_redemptions') }}
    GROUP BY order_id
) cr ON o.id = cr.order_id;
