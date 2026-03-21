-- ============================================================================
-- Layer:       feat
-- Target:      sales_summary
-- Description: Daily sales KPIs — revenue, order count, AOV, units sold,
--              payment method breakdown, fulfillment rates. Powers the Sales
--              dashboard.
-- Sources:     order_enriched
-- ============================================================================

CREATE OR REPLACE TABLE sales_summary AS
SELECT
    CAST(order_date AS DATE)                        AS sale_date,

    -- Volume
    COUNT(DISTINCT order_id)                        AS order_count,
    SUM(total_quantity)                              AS units_sold,

    -- Revenue
    SUM(order_total)                                AS gross_revenue,
    SUM(coupon_discount)                            AS total_coupon_discounts,
    SUM(order_total) - SUM(coupon_discount)         AS net_revenue,
    SUM(tax)                                        AS total_tax,
    SUM(shipping_cost)                              AS total_shipping,

    -- Averages
    ROUND(AVG(order_total), 2)                      AS avg_order_value,
    ROUND(AVG(total_quantity), 2)                   AS avg_items_per_order,

    -- Payment mix
    COUNT(DISTINCT CASE WHEN payment_status = 'paid'    THEN order_id END) AS paid_orders,
    COUNT(DISTINCT CASE WHEN payment_status = 'pending' THEN order_id END) AS pending_orders,
    COUNT(DISTINCT CASE WHEN payment_status = 'failed'  THEN order_id END) AS failed_orders,

    -- Payment methods
    COUNT(DISTINCT CASE WHEN payment_method = 'credit_card' THEN order_id END) AS credit_card_orders,
    COUNT(DISTINCT CASE WHEN payment_method = 'paypal'      THEN order_id END) AS paypal_orders,
    COUNT(DISTINCT CASE WHEN payment_method = 'bank_transfer' THEN order_id END) AS bank_transfer_orders,

    -- Fulfillment
    COUNT(DISTINCT CASE WHEN shipment_status = 'delivered'     THEN order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN shipment_status = 'shipped'       THEN order_id END) AS in_transit_orders,
    COUNT(DISTINCT CASE WHEN shipment_status = 'label_created' THEN order_id END) AS label_created_orders,

    -- Coupons
    COUNT(DISTINCT CASE WHEN coupon_discount > 0 THEN order_id END) AS orders_with_coupon

FROM order_enriched
GROUP BY CAST(order_date AS DATE)
ORDER BY sale_date;
