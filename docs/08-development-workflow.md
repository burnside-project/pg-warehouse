# Development Workflow

pg-warehouse supports two development paths. Both share the same runtime (3-file DuckDB isolation, CDC, reserved schema protection).

| Path | Best For | SQL Convention | Execution Order |
|------|----------|---------------|-----------------|
| **Pipeline path** (`run --pipeline`) | Quick iteration, simple transforms | Numeric prefixes (001_, 002_) | File sort order |
| **Release path** (`build --release`) | Production analytics, team collaboration | `ref()` + `source()` dependencies | DAG resolution |

---

## Architecture (Both Paths)

```
PostgreSQL (source)
   ↓ CDC (continuous, never stops)
raw.duckdb           — LOCKED: CDC black box (stage.* + raw.*)
   ↓ refresh (snapshot)
silver.duckdb
   ├── v0.*          — LOCKED: raw data mirror
   ├── v1.*          — User transforms
   ├── current.*     — Production views
   └── _meta.*       — Metadata (versions, checksums, builds, contracts)
   ↓ feature refresh (copies silver → feature v0)
feature.duckdb
   ├── v0.*          — LOCKED: silver data mirror
   ├── v1.*          — User aggregations
   ├── current.*     — Production views
   └── _meta.*       — Metadata
   ↓
out/*.parquet        — Exported analytics
```

---

# Path 1: Pipeline (Numeric Prefix)

For quick iteration and simple transforms. SQL files use numeric prefixes for ordering.

## Directory Layout

```
~/pg-warehouse/
├── pg-warehouse.yml
├── sql/
│   ├── silver/
│   │   ├── 001_order_enriched.sql
│   │   ├── 002_customer_360.sql
│   │   └── 003_product_catalog.sql
│   └── feat/
│       ├── 001_sales_summary.sql
│       └── 002_customer_analytics.sql
└── out/
```

## SQL Convention

Generic SQL — no schema prefixes. pg-warehouse rewrites `CREATE TABLE` to the target schema and sets the source schema for reads.

```sql
-- sql/silver/001_order_enriched.sql
CREATE OR REPLACE TABLE order_enriched AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.total AS order_total
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id;
```

At runtime: `orders` resolves to `v0.orders`, `order_enriched` becomes `v1.order_enriched`.

## Commands

```bash
# Refresh v0 from CDC raw data
pg-warehouse run --refresh

# Plan what would change
pg-warehouse run --plan --pipeline

# Run everything: refresh → silver → feature → Parquet
pg-warehouse run --refresh --pipeline --promote --version 1

# Run by layer
pg-warehouse run --sql-dir ./sql/silver/
pg-warehouse run --sql-dir ./sql/feat/

# Experiment with v2
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v2

# Promote
pg-warehouse run --promote --version 2
```

## Step-by-Step

```bash
# 1. Get fresh data
pg-warehouse run --refresh

# 2. Plan (terraform-style diff)
pg-warehouse run --plan --sql-dir ./sql/silver/

# 3. Run silver layer
pg-warehouse run --sql-dir ./sql/silver/

# 4. Run feature layer
pg-warehouse run --sql-dir ./sql/feat/

# 5. Promote
pg-warehouse run --promote --version 1

# 6. Verify
pg-warehouse inspect tables
ls -lh out/
```

---

# Path 2: Release (DAG + Contracts)

For production analytics and team collaboration. Models declare dependencies with `ref()`. Releases bundle models into versioned outputs.

## Directory Layout

```
~/pg-warehouse/
├── pg-warehouse.yml
├── contracts/
│   └── silver/
│       └── customer_orders.v1.yml       # data shape contract
├── models/
│   ├── silver/
│   │   └── customer_orders.sql          # curated silver model
│   ├── marts/
│   │   └── customer_ltv.sql             # intermediate transform
│   └── features/
│       └── churn_inputs.sql             # analytics feature
├── releases/
│   └── customer_growth/
│       └── 0.1.0.yml                    # release definition
└── out/
```

## Contracts: What the Data Looks Like

A contract declares the expected shape of a table — columns, types, grain.

`contracts/silver/customer_orders.v1.yml`:
```yaml
contract:
  name: customer_orders
  version: 1
  layer: silver
  description: Curated customer order facts
  grain: one row per order
  primary_key:
    - order_id
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
    - name: order_date
      type: timestamp
      nullable: false
```

Contracts are promises. Downstream models depend on the contract, not the implementation.

## Models: How to Transform Data

Models use `ref()` for model-to-model dependencies and `source()` for system-managed inputs.

`models/silver/customer_orders.sql`:
```sql
-- name: customer_orders
-- materialized: table
-- contract: silver.customer_orders@v1
-- tags: silver,orders

CREATE OR REPLACE TABLE customer_orders AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.total AS order_total,
    o.placed_at AS order_date
FROM {{ source('silver', 'orders') }} o
LEFT JOIN {{ source('silver', 'order_items') }} oi ON o.id = oi.order_id;
```

`models/marts/customer_ltv.sql`:
```sql
-- name: customer_ltv
-- materialized: table
-- tags: marts,customer

CREATE OR REPLACE TABLE customer_ltv AS
SELECT
    customer_id,
    SUM(order_total) AS lifetime_value,
    COUNT(*) AS total_orders,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order
FROM {{ ref('customer_orders') }}
GROUP BY customer_id;
```

`models/features/churn_inputs.sql`:
```sql
-- name: churn_inputs
-- materialized: parquet
-- tags: features,churn

CREATE OR REPLACE TABLE churn_inputs AS
SELECT
    c.customer_id,
    c.lifetime_value,
    c.total_orders,
    DATEDIFF('day', c.last_order, CURRENT_DATE) AS days_since_last
FROM {{ ref('customer_ltv') }} c;
```

### Dependencies (DAG)

`ref()` creates edges. pg-warehouse resolves the execution order automatically:

```
customer_orders  (reads from source — no model deps)
       ↓
  customer_ltv   (ref('customer_orders'))
       ↓
  churn_inputs   (ref('customer_ltv'))
```

No numeric prefixes needed. The graph determines the order.

## Releases: What to Build Together

A release bundles models into a versioned, reproducible build.

`releases/customer_growth/0.1.0.yml`:
```yaml
release:
  name: customer_growth
  version: "0.1.0"
  description: Customer growth analytics
  models:
    - customer_orders
    - customer_ltv
    - churn_inputs
  input:
    contracts:
      - silver.customer_orders@v1
  output:
    target: parquet
  validation:
    fail_on_checksum_drift: true
  promotion:
    allow:
      - dev
      - staging
      - prod
```

## Commands

```bash
# Refresh curated inputs from CDC
pg-warehouse refresh

# Validate everything: contracts, models, DAG, releases
pg-warehouse validate

# Build a release (graph-resolved)
pg-warehouse build --release customer_growth --version 0.1.0

# Show the dependency graph
pg-warehouse graph

# View build history
pg-warehouse history

# Promote to an environment
pg-warehouse promote --release customer_growth --version 0.1.0 --env production

# List contracts and releases
pg-warehouse contracts list
pg-warehouse release list

# Repair metadata
pg-warehouse repair
```

## Step-by-Step: Full Development Cycle

### 1. Define a Contract

```bash
mkdir -p contracts/silver
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
mkdir -p models/silver models/marts models/features

# Silver model (reads from system-managed v0)
cat > models/silver/customer_orders.sql << 'EOF'
-- name: customer_orders
-- materialized: table
-- contract: silver.customer_orders@v1

CREATE OR REPLACE TABLE customer_orders AS
SELECT id AS order_id, customer_id, total AS order_total, placed_at AS order_date
FROM {{ source('silver', 'orders') }};
EOF

# Marts model (depends on silver model via ref)
cat > models/marts/customer_ltv.sql << 'EOF'
-- name: customer_ltv
-- materialized: table

CREATE OR REPLACE TABLE customer_ltv AS
SELECT customer_id, SUM(order_total) AS lifetime_value, COUNT(*) AS total_orders
FROM {{ ref('customer_orders') }}
GROUP BY customer_id;
EOF

# Features model (depends on marts via ref)
cat > models/features/churn_inputs.sql << 'EOF'
-- name: churn_inputs
-- materialized: parquet

CREATE OR REPLACE TABLE churn_inputs AS
SELECT customer_id, lifetime_value, total_orders
FROM {{ ref('customer_ltv') }};
EOF
```

### 3. Define a Release

```bash
mkdir -p releases/customer_growth
cat > releases/customer_growth/0.1.0.yml << 'EOF'
release:
  name: customer_growth
  version: "0.1.0"
  description: Customer growth analytics
  models:
    - customer_orders
    - customer_ltv
    - churn_inputs
  output:
    target: parquet
EOF
```

### 4. Validate

```bash
pg-warehouse validate
```

Expected output:
```
  INFO  Contracts: found 1 files
  OK      OK: contracts/silver/customer_orders.v1.yml (silver.customer_orders@v1)
  INFO  Models: found 3 files
  OK      OK: models/silver/customer_orders.sql (refs: 0, sources: 1)
  OK      OK: models/marts/customer_ltv.sql (refs: 1, sources: 0)
  OK      OK: models/features/churn_inputs.sql (refs: 1, sources: 0)
  OK    Graph: 3 models, no cycles, valid execution order
  INFO  Releases: found 1 files
  OK      OK: customer_growth@0.1.0 (3 models)
  OK    Validation passed: 0 errors, 0 warnings
```

### 5. View the Graph

```bash
pg-warehouse graph
```

Expected output:
```
  INFO  Model DAG (3 nodes)
    1. customer_orders [silver]
    2. customer_ltv [marts]        <- [customer_orders]
    3. churn_inputs [features]     <- [customer_ltv]
```

### 6. Refresh + Build

```bash
# Get fresh data from CDC
pg-warehouse refresh

# Build the release (executes models in DAG order)
pg-warehouse build --release customer_growth --version 0.1.0
```

Expected output:
```
  INFO  Release: customer_growth@0.1.0 (3 models)
  INFO  Plan: 3 steps
    1. customer_orders -> v1.customer_orders (silver)
    2. customer_ltv -> v1.customer_ltv (silver)
    3. churn_inputs -> v1.churn_inputs (feature)
  OK    Build complete: customer_growth@0.1.0
```

### 7. Promote

```bash
pg-warehouse promote --release customer_growth --version 0.1.0 --env production
```

### 8. View History

```bash
pg-warehouse history
```

```
  INFO  Recent Builds
  ID  RELEASE          VERSION  STATUS   MODELS  DURATION  STARTED
  --  -------          -------  ------   ------  --------  -------
  1   customer_growth  0.1.0    success  3       4521ms    2026-03-23 ...

  INFO  Recent Promotions
  RELEASE          VERSION  ENV         BUILD  PROMOTED
  -------          -------  ---         -----  --------
  customer_growth  0.1.0    production  1      2026-03-23 ...
```

---

## Access Guards (Both Paths)

Protected schemas — user SQL cannot write to:

```bash
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v0    # DANGER: v0 is locked
pg-warehouse run --sql-dir ./sql/silver/ --target-schema raw   # DANGER: raw is locked
pg-warehouse run --sql-dir ./sql/silver/ --target-schema _meta # DANGER: _meta is reserved
```

---

## Plan: Preview Changes Before Applying (Pipeline Path)

```bash
# Plan (terraform-style diff)
pg-warehouse run --plan --sql-dir ./sql/silver/

# Output:
Plan: v1 -> v1  (./sql/silver/)
  = order_enriched          UNCHANGED
  ~ customer_360            CHANGED     (validated: ok)
  = product_catalog         UNCHANGED
  + new_model               NEW         (validated: ok)

Summary: 1 new, 1 changed, 2 unchanged, 0 removed  |  validated: 2 ok, 0 warnings
```

---

## Validate: Check Everything Before Building (Release Path)

```bash
pg-warehouse validate
```

Validates:
- Contract YAML files (schema, columns, types)
- Model SQL files (ref/source extraction, header parsing)
- Dependency graph (topological sort, cycle detection)
- Release YAML files (model references, contract references)

---

## Command Reference

### Pipeline Path

| Command | What it does |
|---------|-------------|
| `run --refresh` | Snapshot raw.duckdb → silver.duckdb v0 |
| `run --pipeline` | Run all sql/silver/*.sql + sql/feat/*.sql |
| `run --sql-dir DIR` | Run all SQL in DIR |
| `run --sql-dir DIR --target-schema v2` | Run SQL into v2 (auto-creates) |
| `run --sql-file FILE` | Run single SQL file |
| `run --plan --pipeline` | Plan for full pipeline |
| `run --plan --sql-dir DIR` | Plan for directory |
| `run --promote --version N` | Swap current.* views to vN |
| `run --refresh --pipeline --promote --version 1` | Production: do everything |

### Release Path

| Command | What it does |
|---------|-------------|
| `refresh` | Snapshot raw.duckdb → silver.duckdb v0 |
| `validate` | Check contracts, models, DAG, releases |
| `build --release X --version Y` | Graph-resolved release build |
| `graph` | Show model dependency DAG |
| `history` | Build + promotion history |
| `contracts list` | List data contracts |
| `release list` | List releases |
| `promote --release X --version Y --env E` | Promote to environment |
| `repair` | Fix orphaned builds, stale locks |
