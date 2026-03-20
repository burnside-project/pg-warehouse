"""
E-Commerce Recipe Dashboard — reads feat Parquet files via DuckDB and serves
analytics tiles over a lightweight Flask app.

Includes AI Q&A: natural language questions are translated to SQL using the
data model as grounding context, executed against Parquet files, and answered
in plain English.
"""

import os
import json
from flask import Flask, render_template, jsonify, request
import duckdb

app = Flask(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/data")

# AI Q&A engine (lazy init)
_qa_engine = None


def get_qa_engine():
    global _qa_engine
    if _qa_engine is None:
        from ai_qa import AIQueryEngine
        _qa_engine = AIQueryEngine()
    return _qa_engine


def get_db():
    """Create a read-only in-memory DuckDB connection with Parquet views."""
    db = duckdb.connect(":memory:")
    parquet_files = {
        "sales_summary": "sales_summary.parquet",
        "customer_analytics": "customer_analytics.parquet",
        "product_performance": "product_performance.parquet",
        "promotion_effectiveness": "promotion_effectiveness.parquet",
        "inventory_health": "inventory_health.parquet",
    }
    for table, filename in parquet_files.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            db.execute(
                f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')"
            )
    return db


# ─── API Routes ─────────────────────────────────────────────────────────────


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/sales/overview")
def sales_overview():
    """KPI cards: total revenue, orders, AOV, units."""
    db = get_db()
    row = db.execute("""
        SELECT
            SUM(order_count)       AS total_orders,
            SUM(units_sold)        AS total_units,
            SUM(gross_revenue)     AS total_revenue,
            ROUND(SUM(gross_revenue) / NULLIF(SUM(order_count), 0), 2) AS avg_order_value,
            SUM(delivered_orders)  AS delivered_orders,
            SUM(orders_with_coupon) AS orders_with_coupon
        FROM sales_summary
    """).fetchone()
    return jsonify({
        "total_orders": row[0],
        "total_units": row[1],
        "total_revenue": float(row[2]) if row[2] else 0,
        "avg_order_value": float(row[3]) if row[3] else 0,
        "delivered_orders": row[4],
        "orders_with_coupon": row[5],
    })


@app.route("/api/sales/trend")
def sales_trend():
    """Daily revenue and order count for the trend chart."""
    db = get_db()
    rows = db.execute("""
        SELECT
            sale_date,
            order_count,
            gross_revenue,
            avg_order_value
        FROM sales_summary
        ORDER BY sale_date
    """).fetchall()
    return jsonify([{
        "date": str(r[0]),
        "orders": r[1],
        "revenue": float(r[2]),
        "aov": float(r[3]),
    } for r in rows])


@app.route("/api/sales/payment-mix")
def sales_payment_mix():
    """Payment method breakdown."""
    db = get_db()
    row = db.execute("""
        SELECT
            SUM(credit_card_orders) AS credit_card,
            SUM(paypal_orders)      AS paypal,
            SUM(bank_transfer_orders) AS bank_transfer
        FROM sales_summary
    """).fetchone()
    return jsonify({
        "credit_card": row[0] or 0,
        "paypal": row[1] or 0,
        "bank_transfer": row[2] or 0,
    })


@app.route("/api/sales/fulfillment")
def sales_fulfillment():
    """Fulfillment status breakdown."""
    db = get_db()
    row = db.execute("""
        SELECT
            SUM(delivered_orders)     AS delivered,
            SUM(in_transit_orders)    AS in_transit,
            SUM(label_created_orders) AS label_created,
            SUM(order_count) - SUM(delivered_orders) - SUM(in_transit_orders) - SUM(label_created_orders) AS no_shipment
        FROM sales_summary
    """).fetchone()
    return jsonify({
        "delivered": row[0] or 0,
        "in_transit": row[1] or 0,
        "label_created": row[2] or 0,
        "no_shipment": row[3] or 0,
    })


@app.route("/api/customers/segments")
def customer_segments():
    """Customer segment breakdown."""
    db = get_db()
    rows = db.execute("""
        SELECT
            customer_segment,
            COUNT(*) AS count,
            ROUND(SUM(lifetime_revenue), 2) AS revenue
        FROM customer_analytics
        GROUP BY customer_segment
        ORDER BY revenue DESC
    """).fetchall()
    return jsonify([{
        "segment": r[0],
        "count": r[1],
        "revenue": float(r[2]),
    } for r in rows])


@app.route("/api/customers/activity")
def customer_activity():
    """Activity status breakdown."""
    db = get_db()
    rows = db.execute("""
        SELECT
            activity_status,
            COUNT(*) AS count
        FROM customer_analytics
        GROUP BY activity_status
        ORDER BY count DESC
    """).fetchall()
    return jsonify([{
        "status": r[0],
        "count": r[1],
    } for r in rows])


@app.route("/api/customers/top")
def customer_top():
    """Top 10 customers by lifetime revenue."""
    db = get_db()
    rows = db.execute("""
        SELECT
            customer_name,
            customer_segment,
            activity_status,
            total_orders,
            lifetime_revenue,
            estimated_annual_ltv,
            country
        FROM customer_analytics
        ORDER BY lifetime_revenue DESC
        LIMIT 10
    """).fetchall()
    return jsonify([{
        "name": r[0],
        "segment": r[1],
        "activity": r[2],
        "orders": r[3],
        "revenue": float(r[4]),
        "ltv": float(r[5]) if r[5] else 0,
        "country": r[6],
    } for r in rows])


@app.route("/api/customers/cohorts")
def customer_cohorts():
    """Monthly signup cohort summary."""
    db = get_db()
    rows = db.execute("""
        SELECT
            signup_cohort,
            COUNT(*) AS customers,
            ROUND(AVG(lifetime_revenue), 2) AS avg_revenue,
            ROUND(AVG(total_orders), 1) AS avg_orders
        FROM customer_analytics
        WHERE signup_cohort IS NOT NULL
        GROUP BY signup_cohort
        ORDER BY signup_cohort
    """).fetchall()
    return jsonify([{
        "cohort": str(r[0]),
        "customers": r[1],
        "avg_revenue": float(r[2]),
        "avg_orders": float(r[3]),
    } for r in rows])


@app.route("/api/products/top")
def products_top():
    """Top 15 products by revenue."""
    db = get_db()
    rows = db.execute("""
        SELECT
            product_name,
            category_name,
            total_orders,
            units_sold,
            total_revenue,
            avg_rating,
            stock_status,
            revenue_rank
        FROM product_performance
        ORDER BY revenue_rank
        LIMIT 15
    """).fetchall()
    return jsonify([{
        "name": r[0],
        "category": r[1],
        "orders": r[2],
        "units": r[3],
        "revenue": float(r[4]),
        "rating": float(r[5]) if r[5] else None,
        "stock": r[6],
        "rank": r[7],
    } for r in rows])


@app.route("/api/products/categories")
def products_categories():
    """Revenue by category."""
    db = get_db()
    rows = db.execute("""
        SELECT
            category_name,
            COUNT(*) AS products,
            SUM(total_revenue) AS revenue,
            ROUND(AVG(avg_rating), 2) AS avg_rating
        FROM product_performance
        WHERE category_name IS NOT NULL
        GROUP BY category_name
        ORDER BY revenue DESC
        LIMIT 10
    """).fetchall()
    return jsonify([{
        "category": r[0],
        "products": r[1],
        "revenue": float(r[2]),
        "avg_rating": float(r[3]) if r[3] else None,
    } for r in rows])


@app.route("/api/promotions/overview")
def promotions_overview():
    """Promotion effectiveness summary."""
    db = get_db()
    rows = db.execute("""
        SELECT
            promo_code,
            promo_type,
            promo_status,
            redemption_count,
            unique_customers,
            total_discount_given,
            avg_order_total_with_coupon,
            utilization_pct,
            reach_tier
        FROM promotion_effectiveness
        ORDER BY total_discount_given DESC
        LIMIT 15
    """).fetchall()
    return jsonify([{
        "code": r[0],
        "type": r[1],
        "status": r[2],
        "redemptions": r[3],
        "customers": r[4],
        "discount_given": float(r[5]) if r[5] else 0,
        "avg_order_total": float(r[6]) if r[6] else 0,
        "utilization_pct": float(r[7]) if r[7] else None,
        "reach": r[8],
    } for r in rows])


@app.route("/api/promotions/reach")
def promotions_reach():
    """Reach tier breakdown."""
    db = get_db()
    rows = db.execute("""
        SELECT
            reach_tier,
            COUNT(*) AS count,
            SUM(total_discount_given) AS total_discount
        FROM promotion_effectiveness
        GROUP BY reach_tier
        ORDER BY total_discount DESC
    """).fetchall()
    return jsonify([{
        "tier": r[0],
        "count": r[1],
        "discount": float(r[2]),
    } for r in rows])


@app.route("/api/inventory/alerts")
def inventory_alerts():
    """Products needing reorder."""
    db = get_db()
    rows = db.execute("""
        SELECT
            product_name,
            category_name,
            total_available,
            total_reserved,
            net_available,
            units_sold_last_30d,
            estimated_days_of_stock,
            reorder_signal
        FROM inventory_health
        WHERE reorder_signal IN ('reorder_urgent', 'reorder_soon')
        ORDER BY
            CASE reorder_signal
                WHEN 'reorder_urgent' THEN 0
                ELSE 1
            END,
            units_sold_last_30d DESC
        LIMIT 20
    """).fetchall()
    return jsonify([{
        "name": r[0],
        "category": r[1],
        "available": r[2],
        "reserved": r[3],
        "net_available": r[4],
        "sold_30d": r[5],
        "days_of_stock": float(r[6]) if r[6] else None,
        "signal": r[7],
    } for r in rows])


@app.route("/api/inventory/health")
def inventory_health():
    """Stock health distribution."""
    db = get_db()
    rows = db.execute("""
        SELECT
            reorder_signal,
            COUNT(*) AS count
        FROM inventory_health
        GROUP BY reorder_signal
        ORDER BY count DESC
    """).fetchall()
    return jsonify([{
        "signal": r[0],
        "count": r[1],
    } for r in rows])


# ─── AI Q&A Routes ──────────────────────────────────────────────────────────


@app.route("/api/qa/status")
def qa_status():
    """Check if AI Q&A is available."""
    engine = get_qa_engine()
    return jsonify({
        "available": engine.is_available(),
        "message": "AI Q&A is ready" if engine.is_available()
        else "Set ANTHROPIC_API_KEY to enable AI Q&A",
    })


@app.route("/api/qa/ask", methods=["POST"])
def qa_ask():
    """Ask a natural language question about the data."""
    body = request.get_json()
    question = body.get("question", "").strip()
    if not question:
        return jsonify({"error": "No question provided"}), 400

    engine = get_qa_engine()
    result = engine.ask(question)
    return jsonify(result)


@app.route("/api/qa/suggestions")
def qa_suggestions():
    """Get suggested example questions."""
    engine = get_qa_engine()
    return jsonify(engine.get_suggested_questions())


@app.route("/api/qa/model")
def qa_model():
    """Return the data model context (for transparency)."""
    engine = get_qa_engine()
    return jsonify({"model": engine.model_context})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
