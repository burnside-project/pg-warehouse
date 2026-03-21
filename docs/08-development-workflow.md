# Development Workflow: SQL Pipelines (raw → silver → feat)

This guide covers how to build SQL pipelines using pg-warehouse's Medallion Architecture. You'll learn the directory conventions, how to write silver and feat tables, the CDC pause/resume pattern, and how to run pipelines.

---

## Medallion Architecture

pg-warehouse uses three DuckDB files with versioned schemas:

```
PostgreSQL (source)
   ↓ CDC (continuous)
raw.duckdb           — LOCKED: CDC black box (stage.* + raw.*)
   ↓ --refresh (snapshot)
silver.duckdb
   ├── v0.*          — LOCKED: raw data mirror (populated by --refresh)
   ├── v1.*          — User transforms (reads from v0.*)
   ├── current.*     — Production views (managed by --promote)
   └── _meta.*       — Internal metadata
   ↓ --refresh (pipeline copies silver current.* → feature v0.*)
feature.duckdb
   ├── v0.*          — LOCKED: silver data mirror
   ├── v1.*          — User aggregations (reads from v0.*)
   ├── current.*     — Production views
   └── _meta.*       — Internal metadata
   ↓
out/*.parquet        — Exported analytics
```

| File | Schema | Access | Written By | Purpose |
|------|--------|--------|------------|---------|
| `raw.duckdb` | `raw.*`, `stage.*` | **LOCKED** — DANGER if touched | CDC only | PostgreSQL mirror |
| `silver.duckdb` | `v0.*` | **LOCKED** — refresh only | `--refresh` | Raw data snapshot |
| `silver.duckdb` | `v1.*`, `v2.*` | **User writable** | `--sql-dir`, `--pipeline` | Silver transforms |
| `feature.duckdb` | `v0.*` | **LOCKED** — pipeline only | Internal refresh | Silver data snapshot |
| `feature.duckdb` | `v1.*` | **User writable** | `--sql-dir`, `--pipeline` | Feature aggregations |

**Strict layering rule:** Silver v1 reads from v0 only. Feature v1 reads from v0 only. pg-warehouse enforces this transparently via `SET schema`.

---

## Directory Conventions

```
sql/
├── silver/
│   └── v1/
│       ├── 001_order_enriched.sql
│       ├── 002_customer_360.sql
│       ├── 003_product_catalog.sql
│       ├── 004_promotion_usage.sql
│       └── 005_product_sales.sql
├── feat/
│   ├── 001_sales_summary.sql
│   ├── 002_customer_analytics.sql
│   ├── 003_product_performance.sql
│   ├── 004_promotion_effectiveness.sql
│   └── 005_inventory_health.sql
```

**Rules:**
- **Numeric prefix** (`001_`) = execution order. Files run in lexicographic sort order — no DAG needed.
- **One file = one table.** Filename matches the target table name (minus prefix).
- **Generic SQL** — no schema prefixes needed. pg-warehouse wires source (`v0`) and target (`v1`) schemas transparently.
- `CREATE OR REPLACE TABLE order_enriched AS ...` — pg-warehouse rewrites this to `CREATE OR REPLACE TABLE v1.order_enriched AS ...`
- Use `CREATE OR REPLACE TABLE` for idempotency — safe to re-run at any time.

---

## Writing a Silver Table

Silver tables clean, join, and enrich data from v0 (the raw data snapshot). SQL files are **generic** — no schema prefixes. pg-warehouse sets the source schema (`v0`) and rewrites `CREATE TABLE` to the target schema (`v1`) transparently.

### Template

```sql
-- ============================================================================
-- Layer:       silver
-- Target:      <table_name>
-- Description: <what this table represents>
-- Sources:     <table1>, <table2>, ...
-- ============================================================================

CREATE OR REPLACE TABLE <table_name> AS
SELECT
    -- columns
FROM <primary_table>
LEFT JOIN <secondary_table>
    ON ...
```

### Walkthrough: `order_enriched`

This table denormalizes orders by joining order items, payments, and shipments:

```sql
CREATE OR REPLACE TABLE order_enriched AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.status AS order_status,
    COUNT(DISTINCT oi.id) AS line_item_count,
    SUM(oi.quantity * oi.unit_price) AS gross_amount,
    p.payment_method,
    s.shipped_at,
    s.delivered_at
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
LEFT JOIN payments p ON o.id = p.order_id
LEFT JOIN shipments s ON o.id = s.order_id
GROUP BY o.id, o.customer_id, o.status, p.payment_method, s.shipped_at, s.delivered_at;
```

At execution time, pg-warehouse rewrites this to `CREATE OR REPLACE TABLE v1.order_enriched AS ...` and sets `SET schema = 'v0'` so all unqualified table reads resolve from the raw data mirror.

**Key patterns:**
- Aggregate child tables (order items) into the parent grain (one row per order)
- Use `COALESCE` for nullable aggregates
- Use `DISTINCT ON` subqueries for latest-record-per-key (e.g., latest payment)

---

## Writing a Feat Table

Feat tables produce analytics-ready outputs. They read from v0 (the silver data snapshot). Same generic SQL convention — no schema prefixes.

### Template

```sql
-- ============================================================================
-- Layer:       feat
-- Target:      <table_name>
-- Description: <what this table powers (dashboard, report, ML feature)>
-- Sources:     <table1>, <table2>, ...
-- ============================================================================

CREATE OR REPLACE TABLE <table_name> AS
SELECT
    -- dimensions and metrics
FROM <table>
GROUP BY ...
ORDER BY ...;
```

### Walkthrough: `sales_summary`

Daily sales KPIs aggregated from enriched orders:

```sql
CREATE OR REPLACE TABLE sales_summary AS
SELECT
    CAST(order_date AS DATE) AS sale_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(net_amount) AS net_revenue,
    ROUND(AVG(net_amount), 2) AS avg_order_value
FROM order_enriched
GROUP BY CAST(order_date AS DATE)
ORDER BY sale_date;
```

**Key patterns:**
- Aggregate to the grain your dashboard needs (daily, weekly, per-customer, per-product)
- Use `CASE WHEN` for segmentation and status classification
- Use window functions (`RANK`, `ROW_NUMBER`) for rankings
- Every feat table gets exported to Parquet automatically by the pipeline

---

## Multi-DuckDB: No CDC Pause Needed

In multi-file mode, CDC writes to `raw.duckdb` continuously. Pipelines write to `silver.duckdb` and `feature.duckdb` — separate files, no lock conflict. The `--refresh` command snapshots `raw.duckdb` without stopping CDC.

```
CDC running  →  raw.duckdb locked  →  that's fine, pipeline uses silver.duckdb
--refresh    →  snapshots raw.duckdb to /tmp (filesystem copy, no lock)
--pipeline   →  writes to silver.duckdb and feature.duckdb (no conflict)
```

---

## Running the Pipeline

### Full Pipeline (one command)

```bash
pg-warehouse run --refresh --pipeline --promote --version 1
```

This does: refresh raw → silver v0, run silver SQL, run feat SQL, promote v1 to current.

### Run by Layer

```bash
# Silver layer only
pg-warehouse run --sql-dir ./sql/silver/v1/

# Feature layer only (auto-exports to out/)
pg-warehouse run --sql-dir ./sql/feat/

# Override target schema (e.g., experimenting with v2)
pg-warehouse run --sql-dir ./sql/silver/v1/ --target-schema v2
```

### Run a Single File

```bash
pg-warehouse run --sql-file ./sql/silver/v1/001_order_enriched.sql
```

### Access Guards

```bash
# These will be REJECTED with DANGER messages:
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v0    # v0 is locked
pg-warehouse run --sql-dir ./sql/silver/ --target-schema raw   # raw is locked
pg-warehouse run --sql-dir ./sql/silver/ --target-schema _meta # _meta is reserved
```

---

## Testing with Preview

Use `pg-warehouse preview` to validate SQL without writing to DuckDB:

```bash
# Preview a single file
pg-warehouse preview --sql-file ./sql/feat/001_sales_summary.sql --limit 10

# Preview all feat tables
make pipeline-preview
```

This runs the SELECT portion of your SQL and displays results without creating tables or exporting files. Useful for:
- Validating joins before committing to silver
- Spot-checking aggregation logic in feat tables
- Debugging column types and NULL handling

---

## Adding a New Table: Checklist

### New Silver Table

1. Create `sql/silver/v1/NNN_<table_name>.sql` (next numeric prefix)
2. Use `CREATE OR REPLACE TABLE <table_name> AS SELECT ...` (no schema prefix)
3. Reference source tables without schema prefixes (e.g., `FROM orders`, not `FROM v0.orders`)
4. Add header comment block (layer, target, description, sources)
5. Test: `pg-warehouse preview --sql-file ./sql/silver/v1/NNN_<table_name>.sql --limit 10`
6. Run: `pg-warehouse run --sql-dir ./sql/silver/v1/`

### New Feat Table

1. Create `sql/feat/NNN_<table_name>.sql` (next numeric prefix)
2. Use `CREATE OR REPLACE TABLE <table_name> AS SELECT ...` (no schema prefix)
3. Reference source tables without schema prefixes (e.g., `FROM order_enriched`)
4. Add header comment block
5. Test: `pg-warehouse preview --sql-file ./sql/feat/NNN_<table_name>.sql --limit 10`
6. Run: `pg-warehouse run --sql-dir ./sql/feat/`
7. Verify export: `ls -lh ./out/<table_name>.parquet`

### Verification

```bash
# Check run history
sqlite3 .pgwh/state.db 'SELECT * FROM feature_runs ORDER BY started_at DESC LIMIT 5;'

# Verify Parquet output
ls -lh ./out/
```
