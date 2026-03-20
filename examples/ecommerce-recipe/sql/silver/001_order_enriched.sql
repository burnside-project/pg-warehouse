-- ============================================================================
-- Layer:       silver
-- Target:      silver.order_enriched
-- Description: Denormalized orders combining order header, line items, payments,
--              shipments, and coupon redemptions into a single analytics-ready
--              table. One row per order.
-- Sources:     raw.orders, raw.order_items, raw.payments, raw.shipments,
--              raw.coupon_redemptions
-- ============================================================================

CREATE OR REPLACE TABLE silver.order_enriched AS
SELECT
    o.id                                            AS order_id,
    o.customer_id,
    o.address_id,
    o.status                                        AS order_status,
    o.placed_at                                     AS order_date,
    o.updated_at                                    AS order_updated_at,

    -- Order totals (from header)
    o.subtotal,
    o.tax,
    o.shipping                                      AS shipping_cost,
    o.total                                         AS order_total,

    -- Line item aggregates
    COALESCE(oi.line_item_count, 0)                 AS line_item_count,
    COALESCE(oi.total_qty, 0)                       AS total_quantity,
    COALESCE(oi.items_subtotal, 0)                  AS items_subtotal,

    -- Payment info (latest payment per order)
    p.payment_method,
    p.payment_status,
    p.payment_amount,
    p.gateway_txn_id,
    p.settled_at                                    AS payment_settled_at,

    -- Shipment info (latest shipment per order)
    s.carrier,
    s.tracking_number,
    s.shipment_status,
    s.shipped_at,
    s.delivered_at,

    -- Coupon / discount
    COALESCE(cr.coupon_discount, 0)                 AS coupon_discount,
    cr.coupon_count

FROM raw.orders o

-- Line items aggregate
LEFT JOIN (
    SELECT
        order_id,
        COUNT(*)                                    AS line_item_count,
        SUM(qty)                                    AS total_qty,
        SUM(line_total)                             AS items_subtotal
    FROM raw.order_items
    GROUP BY order_id
) oi ON o.id = oi.order_id

-- Latest payment per order
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        method                                      AS payment_method,
        status                                      AS payment_status,
        amount                                      AS payment_amount,
        gateway_txn_id,
        settled_at
    FROM raw.payments
    ORDER BY order_id, created_at DESC
) p ON o.id = p.order_id

-- Latest shipment per order
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        carrier,
        tracking_number,
        status                                      AS shipment_status,
        shipped_at,
        delivered_at
    FROM raw.shipments
    ORDER BY order_id, id DESC
) s ON o.id = s.order_id

-- Coupon redemptions aggregate
LEFT JOIN (
    SELECT
        order_id,
        SUM(discount_amount)                        AS coupon_discount,
        COUNT(*)                                    AS coupon_count
    FROM raw.coupon_redemptions
    GROUP BY order_id
) cr ON o.id = cr.order_id;
