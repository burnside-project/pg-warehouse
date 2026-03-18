-- ============================================================================
-- Layer:       feat
-- Target:      feat.sales_summary
-- Description: Daily sales KPIs — revenue, order count, AOV, units sold,
--              payment method breakdown. Powers the Sales dashboard.
-- Sources:     silver.order_enriched
-- ============================================================================

CREATE OR REPLACE TABLE feat.sales_summary AS
SELECT
    CAST(order_date AS DATE)                        AS sale_date,

    -- Volume
    COUNT(DISTINCT order_id)                        AS order_count,
    SUM(total_quantity)                              AS units_sold,

    -- Revenue
    SUM(gross_amount)                               AS gross_revenue,
    SUM(total_discount)                             AS total_discounts,
    SUM(net_amount)                                 AS net_revenue,

    -- Averages
    ROUND(AVG(net_amount), 2)                       AS avg_order_value,
    ROUND(AVG(total_quantity), 2)                   AS avg_items_per_order,

    -- Payment mix
    COUNT(DISTINCT CASE WHEN payment_status = 'paid'    THEN order_id END) AS paid_orders,
    COUNT(DISTINCT CASE WHEN payment_status = 'pending' THEN order_id END) AS pending_orders,
    COUNT(DISTINCT CASE WHEN payment_status = 'failed'  THEN order_id END) AS failed_orders,

    -- Fulfillment
    COUNT(DISTINCT CASE WHEN shipment_status = 'delivered' THEN order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN shipment_status = 'shipped'   THEN order_id END) AS in_transit_orders

FROM silver.order_enriched
GROUP BY CAST(order_date AS DATE)
ORDER BY sale_date;
