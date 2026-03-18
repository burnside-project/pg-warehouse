-- ============================================================================
-- Layer:       silver
-- Target:      silver.order_enriched
-- Description: Enriched orders combining orders, order items, payments, and
--              shipments into a single denormalized table for downstream analytics.
-- Sources:     raw.orders, raw.order_items, raw.payments, raw.shipments
-- ============================================================================

CREATE OR REPLACE TABLE silver.order_enriched AS
SELECT
    o.id                                        AS order_id,
    o.customer_id,
    o.status                                    AS order_status,
    o.created_at                                AS order_date,
    o.updated_at                                AS order_updated_at,

    -- Order item aggregates
    COUNT(DISTINCT oi.id)                       AS line_item_count,
    COALESCE(SUM(oi.quantity), 0)               AS total_quantity,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS gross_amount,
    COALESCE(SUM(oi.discount_amount), 0)        AS total_discount,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0)
        - COALESCE(SUM(oi.discount_amount), 0)  AS net_amount,

    -- Payment info (latest payment per order)
    p.payment_method,
    p.payment_status,
    p.paid_at,

    -- Shipment info (latest shipment per order)
    s.carrier,
    s.tracking_number,
    s.shipped_at,
    s.delivered_at,
    s.shipment_status

FROM raw.orders o
LEFT JOIN raw.order_items oi
    ON o.id = oi.order_id
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        payment_method,
        status AS payment_status,
        paid_at
    FROM raw.payments
    ORDER BY order_id, created_at DESC
) p ON o.id = p.order_id
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        carrier,
        tracking_number,
        shipped_at,
        delivered_at,
        status AS shipment_status
    FROM raw.shipments
    ORDER BY order_id, created_at DESC
) s ON o.id = s.order_id
GROUP BY
    o.id, o.customer_id, o.status, o.created_at, o.updated_at,
    p.payment_method, p.payment_status, p.paid_at,
    s.carrier, s.tracking_number, s.shipped_at, s.delivered_at, s.shipment_status;
