# Data Model and AI Q&A

## The Data Modeling Moat

The `models/` directory contains a machine-readable **semantic layer** — YAML files that describe every entity, column, metric, relationship, and business rule in the analytics warehouse. This serves two purposes:

1. **Documentation**: Humans read it to understand the data model
2. **AI Grounding**: LLMs read it to translate natural language questions into accurate SQL

The data model is the "moat" — structured domain knowledge that makes AI answers correct and consistent.

## Model Structure

```
models/
├── manifest.yml              # Master registry: layers, relationships, business rules
├── entities/                 # Entity definitions (one per silver table)
│   ├── order.yml             # Columns, types, synonyms, enums, units
│   ├── customer.yml
│   ├── product.yml
│   ├── promotion.yml
│   └── inventory.yml
└── metrics/                  # Metric definitions (one per feat table)
    ├── sales.yml             # Measures, dimensions, derived metrics
    ├── customer_analytics.yml
    ├── product_performance.yml
    ├── promotion_effectiveness.yml
    └── inventory_health.yml
```

## Entity Definition Format

Each entity YAML describes a silver or feat table:

```yaml
name: order
display_name: Order
description: Denormalized order combining header, line items, payments, shipments
table: silver.order_enriched
grain: one row per order
primary_key: order_id

columns:
  - name: order_total
    type: numeric
    description: "Final order amount: subtotal + tax + shipping"
    unit: USD
    synonyms: [total, amount, revenue]

  - name: order_status
    type: text
    description: Current order status
    enum: [pending, confirmed, shipped, delivered, cancelled]

  - name: payment_method
    type: text
    description: Payment method used
    enum: [credit_card, paypal, bank_transfer]
    synonyms: [pay_method, payment_type]

example_questions:
  - "How many orders were placed last month?"
  - "What is the average order value?"
```

### Key Features

| Feature | Example | Why It Matters |
|---------|---------|----------------|
| **Synonyms** | `total` also: amount, revenue | Users don't need exact column names |
| **Enums** | `order_status`: pending, shipped... | AI generates valid WHERE clauses |
| **Units** | `[USD]`, `[percent]` | AI formats answers correctly |
| **Grain** | "one row per order" | Prevents double-counting |
| **PII flags** | `pii: true` on email, name | AI can warn about sensitive data |

## Metric Definition Format

Metrics describe feat-level aggregations:

```yaml
name: sales
display_name: Sales Metrics
table: feat.sales_summary
grain: one row per calendar date
time_column: sale_date

measures:
  - name: gross_revenue
    type: numeric
    description: Sum of order totals
    aggregation: SUM
    unit: USD
    synonyms: [revenue, sales, gmv]

derived_metrics:
  - name: payment_success_rate
    description: Percentage of orders with successful payment
    formula: "paid_orders * 100.0 / NULLIF(order_count, 0)"
    unit: percent
```

Derived metrics are formulas the AI can compute on the fly, even if they're not materialized in the Parquet file.

## Business Rules

The manifest defines business rules that apply across tables:

```yaml
business_rules:
  - name: revenue_definition
    description: >
      Revenue is subtotal + tax + shipping. Net revenue subtracts coupon
      discounts. Do NOT use line item sums for revenue.
    applies_to: [feat.sales_summary, feat.customer_analytics]

  - name: customer_segmentation
    description: >
      never_purchased (0 orders), one_time (1), occasional (2-3),
      regular (4-10), loyal (11+).
    applies_to: [feat.customer_analytics]
```

The AI reads these rules and applies them when generating SQL. If someone asks "What is our revenue?", the AI uses `order_total` (subtotal + tax + shipping), not `line_total` sum.

## AI Q&A Flow

```
User: "What is our churn rate?"
         │
         ▼
┌────────────────────────┐
│ Load Data Model        │  manifest.yml + entities/*.yml + metrics/*.yml
│ Build System Prompt    │  → columns, synonyms, enums, business rules
└────────────────────────┘
         │
         ▼
┌────────────────────────┐
│ LLM: Generate SQL      │  Uses model context to pick correct table,
│                        │  column names, and aggregation logic
│                        │  → SELECT COUNT(CASE WHEN activity_status
│                        │     = 'churned' THEN 1 END) * 100.0 / COUNT(*)
│                        │     FROM customer_analytics
└────────────────────────┘
         │
         ▼
┌────────────────────────┐
│ DuckDB: Execute SQL    │  Read Parquet files in-memory
│                        │  → [{churn_rate: 34.2}]
└────────────────────────┘
         │
         ▼
┌────────────────────────┐
│ LLM: Format Answer     │  "The current churn rate is 34.2%, meaning
│                        │   about 1 in 3 customers haven't ordered
│                        │   in over 180 days."
└────────────────────────┘
         │
         ▼
Dashboard renders: answer + SQL + raw data table
```

## Versioned Models

When silver versions change, the data model should be versioned too:

```
models/
├── v2/                        ← current production model
│   └── entities/customer.yml  # customer_segment has 4 tiers
├── v3/                        ← experiment model
│   └── entities/customer.yml  # customer_segment has 5 tiers (added 'regular')
```

The AI loads the model for the active production version. This ensures answers match the current data definitions.

## Dashboard Integration

The AI Q&A is available as a tab in the dashboard alongside the chart-based tiles:

| Dashboard Tab | Data Source | Interaction |
|---------------|-------------|-------------|
| Sales | `feat.sales_summary` | Charts + KPI cards |
| Customers | `feat.customer_analytics` | Charts + tables |
| Products | `feat.product_performance` | Charts + tables |
| Promotions | `feat.promotion_effectiveness` | Charts + tables |
| Inventory | `feat.inventory_health` | Charts + tables |
| **Ask AI** | All feat tables via DuckDB | Natural language Q&A |

The dashboard reads Parquet files (not DuckDB directly), while the AI Q&A reads the same Parquet files via in-memory DuckDB views. Both are read-only.

## Example: E-Commerce Recipe

See `examples/ecommerce-recipe/` for a complete working example with:
- 14 source tables (orders, customers, products, etc.)
- 4 silver transforms + 5 feat aggregations
- Full data model (5 entities, 5 metric definitions, 6 business rules)
- Docker-based dashboard with AI Q&A tab
- 25+ curated example questions across 5 domains

## Running AI Q&A

```bash
# Set API key
export ANTHROPIC_API_KEY=sk-ant-...

# Run dashboard with AI enabled
cd examples/ecommerce-recipe/dashboard
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY docker compose up --build

# Open http://localhost:8050 → click "Ask AI" tab
```

Without an API key, the dashboard works normally (all chart tabs function). Only the Ask AI tab requires the key.
