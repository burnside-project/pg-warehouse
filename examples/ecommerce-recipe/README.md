# Recipe: E-Commerce Pipeline

Medallion Architecture pipeline (raw → silver → feat) for an e-commerce PostgreSQL database.

This recipe demonstrates how to build a complete analytics pipeline using pg-warehouse's development workflow, producing dashboard-ready Parquet files from a normalized e-commerce schema.

## Prerequisites

- A PostgreSQL database with the e-commerce schema (see [Schema](#source-schema) below)
- `pg-warehouse` binary built (`make build` from project root)
- PostgreSQL credentials with read access

## Quickstart

```bash
# 1. Build the binary (from project root)
make build

# 2. Copy and edit the config with your connection details
cp examples/ecommerce-recipe/pg-warehouse.yml.example pg-warehouse.yml
# Edit pg-warehouse.yml with your PostgreSQL connection string

# 3. Initialize the warehouse
./pg-warehouse init --config pg-warehouse.yml

# 4. Sync source tables into raw.*
./pg-warehouse sync --config pg-warehouse.yml

# 5. Preview feat tables (no writes)
./examples/ecommerce-recipe/run-pipeline.sh --preview

# 6. Run full pipeline (silver → feat → parquet)
./examples/ecommerce-recipe/run-pipeline.sh
```

## Source Schema

This recipe expects the following e-commerce tables in the source PostgreSQL database:

### Core Tables (~14 tables)

| Table | Description | PK | Key FKs |
|-------|-------------|-----|---------|
| `customers` | Customer accounts | id | — |
| `addresses` | Shipping/billing addresses | id | → customers.id |
| `orders` | Order headers | id | → customers.id, addresses.id |
| `order_items` | Line items per order | id | → orders.id, product_variants.id |
| `payments` | Payment transactions | id | → orders.id |
| `shipments` | Shipment tracking | id | → orders.id |
| `products` | Product definitions | id | → categories.id |
| `product_variants` | Size/color/SKU variants | id | → products.id |
| `categories` | Product categories (self-referencing) | id | → categories.id |
| `inventory` | Stock levels per variant | variant_id | → product_variants.id |
| `price_history` | Historical price changes | id | → product_variants.id |
| `reviews` | Customer product reviews | id | → products.id, customers.id |
| `promotions` | Coupon/discount definitions | id | — |
| `coupon_redemptions` | Coupon usage records | id | → promotions.id, orders.id, customers.id |

### Expected Column Names

<details>
<summary>Click to expand full column reference</summary>

**customers:** id, email, name, password_hash, last_login, created_at

**addresses:** id, customer_id, addr_type, line1, line2, city, state, zip, country, is_default, created_at

**orders:** id, customer_id, address_id, status, subtotal, tax, shipping, total, placed_at, updated_at

**order_items:** id, order_id, variant_id, qty, unit_price, line_total

**payments:** id, order_id, method, status, amount, gateway_txn_id, created_at, settled_at

**shipments:** id, order_id, carrier, tracking_number, status, shipped_at, delivered_at

**products:** id, category_id, name, slug, description, base_price, status, created_at

**product_variants:** id, product_id, sku, name, price_override, weight_grams, created_at

**categories:** id, parent_id, name, slug, position, created_at

**inventory:** variant_id, warehouse_id, qty_available, qty_reserved, updated_at

**price_history:** id, variant_id, old_price, new_price, changed_at

**reviews:** id, product_id, customer_id, rating, title, body, created_at

**promotions:** id, code, promo_type, value, min_order, max_uses, uses, starts_at, ends_at

**coupon_redemptions:** id, promotion_id, order_id, customer_id, discount_amount, redeemed_at

</details>

## Business Questions

This recipe answers the following analytics questions, organized by dashboard:

### Sales — *How is the business performing day-over-day?*

| Question | Feature Table | Dashboard Tile |
|----------|---------------|----------------|
| What is our total revenue, order count, and average order value? | `feat.sales_summary` | KPI cards (Revenue, Orders, AOV, Units) |
| What does our revenue trajectory look like? | `feat.sales_summary` | Revenue Trend (line chart: revenue + orders over time) |
| How do customers prefer to pay? | `feat.sales_summary` | Payment Methods (doughnut: credit card / PayPal / bank transfer) |
| What percentage of orders are delivered? | `feat.sales_summary` | Fulfillment (doughnut: delivered / in-transit / label created) |

### Customers — *Who are our customers and how engaged are they?*

| Question | Feature Table | Dashboard Tile |
|----------|---------------|----------------|
| How are customers distributed by purchase frequency? | `feat.customer_analytics` | Customer Segments (bar chart: loyal / regular / occasional / one-time) |
| How many customers are at risk of churning? | `feat.customer_analytics` | Activity Status (doughnut: active / cooling / at-risk / churned) |
| Who are our most valuable customers? | `feat.customer_analytics` | Top Customers (table: name, segment, revenue, LTV) |
| Which signup months produce the highest-value customers? | `feat.customer_analytics` | Signup Cohorts (bar chart: avg revenue + count per cohort) |

### Products — *Which products drive the most revenue?*

| Question | Feature Table | Dashboard Tile |
|----------|---------------|----------------|
| What are our best sellers? | `feat.product_performance` | Top Products (table: rank, name, orders, units, revenue, rating) |
| Which categories generate the most revenue? | `feat.product_performance` | Revenue by Category (horizontal bar chart) |

### Promotions — *Which promotions are most effective?*

| Question | Feature Table | Dashboard Tile |
|----------|---------------|----------------|
| How many customers do our promos reach? | `feat.promotion_effectiveness` | Reach Tiers (doughnut: high / medium / low reach) |
| Where is the discount budget going? | `feat.promotion_effectiveness` | Discount by Reach (bar chart) |
| What is the ROI per promotion code? | `feat.promotion_effectiveness` | Promotion Details (table: code, redemptions, discount given, avg order) |

### Inventory — *Which products need restocking?*

| Question | Feature Table | Dashboard Tile |
|----------|---------------|----------------|
| What is the overall inventory health distribution? | `feat.inventory_health` | Stock Health (bar chart: healthy / reorder soon / reorder urgent) |
| Which products need immediate restocking? | `feat.inventory_health` | Reorder Alerts (table: product, available, velocity, days-of-stock) |

## Pipeline: Schema Mapping

### Silver Layer (curated, joined)

| Silver Table | Description | Raw Sources |
|-------------|-------------|-------------|
| `silver.order_enriched` | Denormalized orders (1 row/order) with items, payments, shipments, coupons | orders, order_items, payments, shipments, coupon_redemptions |
| `silver.customer_360` | Customer profiles with lifetime order metrics, address, and review activity | customers, orders, order_items, addresses, reviews |
| `silver.product_catalog` | Products with variants, inventory, price history, and review metrics | products, product_variants, categories, inventory, price_history, reviews |
| `silver.promotion_usage` | Promotion definitions with redemption metrics and order impact | promotions, coupon_redemptions, orders |

### Feat Layer (analytics-ready, exported to Parquet)

| Feat Table | Description | Dashboard |
|-----------|-------------|-----------|
| `feat.sales_summary` | Daily KPIs: revenue, AOV, payment mix, fulfillment rates | Sales |
| `feat.customer_analytics` | Cohort analysis, LTV estimates, segmentation, activity status | Customer |
| `feat.product_performance` | Product rankings by revenue, volume, and rating | Product |
| `feat.promotion_effectiveness` | Promotion ROI, utilization rates, reach tiers | Marketing |
| `feat.inventory_health` | Stock levels, sell-through velocity, reorder signals | Operations |

## Parquet Outputs

After a full pipeline run, the following files are produced in `./out/`:

```
out/
├── sales_summary.parquet
├── customer_analytics.parquet
├── product_performance.parquet
├── promotion_effectiveness.parquet
└── inventory_health.parquet
```

## Dashboard

A self-contained Docker dashboard reads the Parquet outputs and renders interactive tiles with Chart.js.

### Run the Dashboard

```bash
# From examples/ecommerce-recipe/dashboard/
cd examples/ecommerce-recipe/dashboard

# Option 1: Docker Compose (recommended)
docker compose up --build
# Dashboard available at http://localhost:8050

# Option 2: Point to a custom data directory
DATA_DIR=/path/to/parquet/files docker compose up --build

# Option 3: Run locally without Docker
pip install -r requirements.txt
DATA_DIR=../out python app.py
```

### Dashboard Pages

| Page | Feature Table | Tiles |
|------|---------------|-------|
| **Sales** | `sales_summary.parquet` | KPI cards, Revenue trend, Payment mix, Fulfillment status |
| **Customers** | `customer_analytics.parquet` | Segments, Activity status, Top customers, Signup cohorts |
| **Products** | `product_performance.parquet` | Top products table, Revenue by category |
| **Promotions** | `promotion_effectiveness.parquet` | Reach tiers, Discount distribution, Promo details table |
| **Inventory** | `inventory_health.parquet` | Stock health distribution, Reorder alerts table |

### Architecture

```
Parquet files (out/)  →  DuckDB (in-memory)  →  Flask API  →  Chart.js frontend
                                                    ↑
                                           Data Model (YAML)
                                                    ↓
                                     Claude API  →  AI Q&A tab
```

The dashboard is **read-only** — it creates in-memory DuckDB views over Parquet files on each request, with no persistent state.

## Data Model (The Moat)

The `models/` directory contains a machine-readable semantic layer that describes every entity, column, metric, relationship, and business rule. This is the **data modeling moat** — structured knowledge that:

1. **Grounds AI answers** — the Q&A engine loads these YAML files as context so Claude generates accurate, schema-aware SQL
2. **Documents the domain** — humans read it to understand what each column means, how metrics are calculated, and what business rules apply
3. **Enables synonyms** — users can say "revenue" or "sales" or "GMV" and the AI maps it to the correct column
4. **Encodes business rules** — segmentation thresholds, LTV formulas, and reorder signals are defined once and applied consistently

### Model Structure

```
models/
├── manifest.yml                    # Master registry: layers, entities, relationships, business rules
├── entities/
│   ├── order.yml                   # 30 columns, synonyms, enums, units
│   ├── customer.yml                # Demographics, lifetime metrics, segments
│   ├── product.yml                 # Catalog, variants, inventory, reviews
│   ├── promotion.yml               # Promo definitions, redemption metrics
│   └── inventory.yml               # Stock levels, velocity, reorder signals
└── metrics/
    ├── sales.yml                   # Daily KPIs, derived metrics (payment success rate, delivery rate)
    ├── customer_analytics.yml      # Segments, LTV, churn rate, repeat purchase rate
    ├── product_performance.yml     # Revenue/volume/rating rankings
    ├── promotion_effectiveness.yml # ROI, utilization, discount per customer
    └── inventory_health.yml        # Stock health, sell-through rate, inventory value
```

### Key Model Features

| Feature | Example | Why It Matters |
|---------|---------|----------------|
| **Synonyms** | `total` → also: amount, revenue | Users don't need to know exact column names |
| **Enums** | `order_status`: pending, confirmed, shipped... | AI generates valid WHERE clauses |
| **Units** | `[USD]`, `[percent]` | AI formats answers correctly |
| **Business Rules** | "Revenue = subtotal + tax + shipping" | AI uses the right calculation |
| **Derived Metrics** | `churn_rate = COUNT(churned) / COUNT(*)` | AI can compute metrics not in the Parquet files |
| **Grain** | "one row per order" | AI avoids double-counting |

## AI Q&A

The **Ask AI** tab lets users ask natural language questions about their e-commerce data. The AI engine:

1. Loads the data model YAMLs as grounding context
2. Uses Claude to generate a DuckDB SQL query
3. Executes the query against Parquet files
4. Uses Claude to format the results into a natural language answer

### Enable AI Q&A

```bash
# Set your Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-...

# Docker Compose
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY docker compose up --build

# Local
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY DATA_DIR=../out python app.py
```

The dashboard works without an API key — the Ask AI tab simply shows a message that the key is required.

### Example Questions

| Domain | Question |
|--------|----------|
| Sales | "What is our total revenue this month?" |
| Sales | "What percentage of orders are delivered?" |
| Customers | "How many loyal customers do we have?" |
| Customers | "What is the churn rate?" |
| Products | "What are the top 10 products by revenue?" |
| Products | "Show products with high sales but low ratings" |
| Promotions | "Which promotion gave the most discount?" |
| Inventory | "Which products urgently need reordering?" |
| Cross-domain | "What is the average LTV of customers who used a coupon?" |

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/qa/status` | GET | Check if AI is configured |
| `/api/qa/ask` | POST | `{"question": "..."}` → SQL + answer |
| `/api/qa/suggestions` | GET | Curated example questions |
| `/api/qa/model` | GET | View the data model context |

### How It Works (Technical)

```
User Question
    ↓
┌──────────────────────┐
│ Load Data Model      │  manifest.yml + entities/*.yml + metrics/*.yml
│ Build System Prompt  │  → column names, synonyms, enums, business rules
└──────────────────────┘
    ↓
┌──────────────────────┐
│ Claude: Generate SQL │  "What is our churn rate?"
│                      │  → SELECT COUNT(CASE WHEN activity_status = 'churned'...)
└──────────────────────┘
    ↓
┌──────────────────────┐
│ DuckDB: Execute SQL  │  Read Parquet files in-memory
│                      │  → [{churn_rate: 34.2}]
└──────────────────────┘
    ↓
┌──────────────────────┐
│ Claude: Format Answer│  "The current churn rate is 34.2%. This means..."
└──────────────────────┘
    ↓
Dashboard renders answer + SQL + raw data table
```

## Customization

To adapt this recipe for your own e-commerce database:

1. **Column names differ?** Edit the SQL files in `sql/silver/` to match your schema
2. **Missing tables?** Remove the corresponding silver/feat SQL files and sync entries
3. **Additional tables?** Add new silver SQL files following the naming convention (`NNN_table_name.sql`)
4. **Different metrics?** Edit the feat SQL files to change aggregations or add new KPIs
5. **Dashboard tiles?** Edit `dashboard/app.py` (API routes) and `dashboard/templates/index.html` (tiles)
6. **Data model?** Update `models/entities/*.yml` and `models/metrics/*.yml` to match your schema — the AI Q&A will automatically adapt

See [Development Workflow](../../docs/08-development-workflow.md) for the full template reference.
