# Development Workflow

Build and promote versioned analytical releases from stable curated inputs.

---

## Architecture

```
PostgreSQL (source)
   ↓ CDC (continuous, never stops)
raw.duckdb           — LOCKED: CDC black box
   ↓ pg-warehouse refresh
silver.duckdb
   ├── v0.*          — raw data snapshot (auto-populated)
   ├── v1.*          — your transforms
   ├── current.*     — production views
   └── _meta.*       — builds, contracts, checksums
   ↓ auto-refresh during build
feature.duckdb
   ├── v0.*          — silver data snapshot
   ├── v1.*          — your analytics
   └── _meta.*       — metadata
   ↓
out/*.parquet        — exported artifacts
```

---

## Quick Start (5 minutes)

### 1. Initialize

```bash
pg-warehouse init --config pg-warehouse.yml
```

Creates `raw.duckdb`, `silver.duckdb`, `feature.duckdb`, and scaffolds:
```
models/silver/       — silver layer SQL models
models/features/     — feature layer SQL models
contracts/           — data contracts (YAML)
releases/            — release definitions (YAML)
```

### 2. Write a Model

```sql
-- models/silver/order_enriched.sql
-- name: order_enriched
-- materialized: table

CREATE OR REPLACE TABLE order_enriched AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.total AS order_total,
    o.placed_at AS order_date
FROM {{ source('silver', 'orders') }} o;
```

- `source('silver', 'orders')` reads from v0 (raw data snapshot)
- `CREATE OR REPLACE TABLE` — pg-warehouse rewrites this to target the correct schema
- No schema prefixes needed in your SQL

### 3. Write a Feature That Depends on It

```sql
-- models/features/sales_summary.sql
-- name: sales_summary
-- materialized: parquet

CREATE OR REPLACE TABLE sales_summary AS
SELECT
    CAST(order_date::TIMESTAMP AS DATE) AS sale_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_total) AS gross_revenue
FROM {{ ref('order_enriched') }}
GROUP BY CAST(order_date::TIMESTAMP AS DATE)
ORDER BY sale_date;
```

- `ref('order_enriched')` creates a dependency — pg-warehouse builds `order_enriched` first
- No numeric prefixes — the DAG determines execution order

### 4. Build

```bash
pg-warehouse refresh     # get latest data from CDC
pg-warehouse build       # builds all models in dependency order
```

That's it. Models are materialized, Parquet is exported.

---

## How It Works

### Models

A model is a SQL file in `models/`. One file = one table.

```
models/
├── silver/
│   ├── order_enriched.sql       — reads from source (v0)
│   ├── customer_360.sql         — reads from source (v0)
│   └── product_catalog.sql      — reads from source (v0)
└── features/
    ├── sales_summary.sql        — reads from ref('order_enriched')
    ├── customer_analytics.sql   — reads from ref('customer_360')
    └── product_performance.sql  — reads from ref('product_catalog')
```

#### Header Metadata

```sql
-- name: order_enriched          — model name (defaults to filename)
-- materialized: table           — table, view, or parquet
-- contract: silver.orders@v1    — optional contract this model satisfies
-- tags: silver,orders           — optional tags for filtering
```

#### Dependencies

- `{{ ref('model_name') }}` — depends on another model (DAG edge)
- `{{ source('silver', 'table') }}` — reads from system-managed v0

Models with no `ref()` are root nodes — they run first. The graph determines everything else.

### Contracts

A contract declares what a table looks like — columns, types, grain.

```yaml
# contracts/silver/order_enriched.v1.yml
contract:
  name: order_enriched
  version: 1
  layer: silver
  grain: one row per order
  primary_key: [order_id]
  columns:
    - name: order_id
      type: bigint
      nullable: false
    - name: customer_id
      type: bigint
      nullable: false
    - name: order_total
      type: double
      nullable: true
```

Contracts are optional. They become valuable when teams depend on each other's outputs.

### Releases

A release bundles models into a versioned, reproducible build.

```yaml
# releases/default/0.1.0.yml
release:
  name: default
  version: "0.1.0"
  description: All silver and feature models
  models:
    - order_enriched
    - customer_360
    - product_catalog
    - sales_summary
    - customer_analytics
    - product_performance
  output:
    target: parquet
```

Releases are optional for simple projects. `pg-warehouse build` without `--release` builds all discovered models.

### The DAG

```
order_enriched   (source only, no deps)
customer_360     (source only, no deps)
product_catalog  (source only, no deps)
       ↓                ↓              ↓
sales_summary    customer_analytics    product_performance
(ref: order_enriched)  (ref: customer_360)  (ref: product_catalog)
```

`pg-warehouse graph` shows this:
```
  INFO  Model DAG (6 nodes)
    1. order_enriched [silver]
    2. customer_360 [silver]
    3. product_catalog [silver]
    4. sales_summary [features]     <- [order_enriched]
    5. customer_analytics [features] <- [customer_360]
    6. product_performance [features] <- [product_catalog]
```

---

## Commands

### Core Workflow

```bash
pg-warehouse refresh                    # snapshot raw → silver v0
pg-warehouse validate                   # check contracts, models, DAG, releases
pg-warehouse build                      # build all models in DAG order
pg-warehouse promote --release default --version 0.1.0 --env prod
```

### Build Options

```bash
# Build all models (discovers models/, builds in DAG order)
pg-warehouse build

# Build a specific release
pg-warehouse build --release customer_growth --version 0.1.0

# Build a single model + its dependencies
pg-warehouse build --select customer_analytics
```

### Inspection

```bash
pg-warehouse graph                      # show model dependency DAG
pg-warehouse history                    # show build and promotion history
pg-warehouse contracts list             # list data contracts
pg-warehouse release list               # list releases
pg-warehouse inspect tables             # list all DuckDB tables
```

### Maintenance

```bash
pg-warehouse repair                     # fix orphaned builds, stale locks
pg-warehouse cdc status                 # check CDC health
```

---

## Step-by-Step: Full Development Cycle

### 1. Define a Contract (optional)

```bash
cat > contracts/silver/customer_orders.v1.yml << 'EOF'
contract:
  name: customer_orders
  version: 1
  layer: silver
  grain: one row per order
  primary_key: [order_id]
  columns:
    - name: order_id
      type: bigint
      nullable: false
    - name: customer_id
      type: bigint
      nullable: false
    - name: order_total
      type: double
      nullable: true
EOF
```

### 2. Write Models

```bash
# Silver model (reads from raw data via source)
cat > models/silver/customer_orders.sql << 'EOF'
-- name: customer_orders
-- materialized: table

CREATE OR REPLACE TABLE customer_orders AS
SELECT id AS order_id, customer_id, total AS order_total
FROM {{ source('silver', 'orders') }};
EOF

# Feature model (reads from silver model via ref)
cat > models/features/customer_ltv.sql << 'EOF'
-- name: customer_ltv
-- materialized: parquet

CREATE OR REPLACE TABLE customer_ltv AS
SELECT customer_id, SUM(order_total) AS lifetime_value
FROM {{ ref('customer_orders') }}
GROUP BY customer_id;
EOF
```

### 3. Validate

```bash
pg-warehouse validate
```

```
  INFO  Contracts: found 1 files
  OK      OK: contracts/silver/customer_orders.v1.yml (silver.customer_orders@v1)
  INFO  Models: found 2 files
  OK      OK: models/silver/customer_orders.sql (refs: 0, sources: 1)
  OK      OK: models/features/customer_ltv.sql (refs: 1, sources: 0)
  OK    Graph: 2 models, no cycles, valid execution order
  OK    Validation passed
```

### 4. View the Graph

```bash
pg-warehouse graph
```

```
  INFO  Model DAG (2 nodes)
    1. customer_orders [silver]
    2. customer_ltv [features]     <- [customer_orders]
```

### 5. Refresh + Build

```bash
pg-warehouse refresh     # get latest data from CDC
pg-warehouse build       # builds customer_orders first, then customer_ltv
```

### 6. Promote

```bash
pg-warehouse promote --release default --version 0.1.0 --env production
```

### 7. Verify

```bash
pg-warehouse history                    # see build record
pg-warehouse inspect tables             # see materialized tables
ls -lh out/                             # see Parquet exports
```

---

## Adding a New Model

1. Create `models/silver/my_model.sql` or `models/features/my_model.sql`
2. Use `{{ source('silver', 'table') }}` for raw data reads
3. Use `{{ ref('other_model') }}` for model-to-model dependencies
4. Add header: `-- name: my_model` and `-- materialized: table`
5. Validate: `pg-warehouse validate`
6. Build: `pg-warehouse build`

No numeric prefixes. No release YAML changes (unless using named releases). Just write SQL and build.

---

## Access Guards

Protected schemas — user models cannot write to:

| Schema | Protection |
|--------|-----------|
| `raw` | DANGER — CDC only |
| `stage` | DANGER — CDC internal |
| `v0` | DANGER — populated by refresh only |
| `_meta` | DANGER — pg-warehouse internal |

---

## Command Reference

| Command | What it does |
|---------|-------------|
| `refresh` | Snapshot raw.duckdb → silver.duckdb v0 |
| `validate` | Check contracts, models, DAG, releases |
| `build` | Build all models in DAG order |
| `build --release X --version Y` | Build a specific release |
| `build --select model_name` | Build one model + its deps |
| `graph` | Show model dependency DAG |
| `history` | Build + promotion history |
| `contracts list` | List data contracts |
| `release list` | List releases |
| `promote --release X --version Y --env E` | Promote to environment |
| `repair` | Fix orphaned builds, stale locks |
| `inspect tables` | List all DuckDB tables |
| `cdc status` | Check CDC health |
