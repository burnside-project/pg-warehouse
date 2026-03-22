# Development Workflow: SQL Pipelines (raw → silver → feat)

This guide covers how to build SQL pipelines using pg-warehouse's Medallion Architecture. You'll learn the directory conventions, how to write silver and feat tables, how to plan changes before applying, and how to run pipelines.

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
│   ├── 001_order_enriched.sql
│   ├── 002_customer_360.sql
│   ├── 003_product_catalog.sql
│   ├── 004_promotion_usage.sql
│   └── 005_product_sales.sql
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

This does: refresh raw → silver v0, run silver SQL, run feat SQL + Parquet export, promote v1 to current.

### Run by Layer

```bash
# Silver layer only
pg-warehouse run --sql-dir ./sql/silver/

# Feature layer only (auto-exports to out/)
pg-warehouse run --sql-dir ./sql/feat/

# Override target schema (e.g., experimenting with v2)
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v2
```

### Run a Single File

```bash
pg-warehouse run --sql-file ./sql/silver/001_order_enriched.sql
```

### Access Guards

```bash
# These will be REJECTED with DANGER messages:
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v0    # v0 is locked
pg-warehouse run --sql-dir ./sql/silver/ --target-schema raw   # raw is locked
pg-warehouse run --sql-dir ./sql/silver/ --target-schema _meta # _meta is reserved
```

---

## Plan: Preview Changes Before Applying

Use `--plan` to see what will change **before** running SQL — similar to `terraform plan`.

### How It Works

`--plan` compares your SQL files against the currently active version's checksums. For each file, it reports:

```
+ NEW        — SQL file exists but table wasn't in previous version
~ CHANGED    — SQL file content differs from what built the current version
= UNCHANGED  — SQL file is identical to what built the current version
- REMOVED    — Table exists in current version but no SQL file for it
```

### Usage

```bash
# Plan silver changes (compare against current active version)
pg-warehouse run --plan --sql-dir ./sql/silver/

# Output:
Plan: v1 (active) → v2

  = order_enriched          UNCHANGED  (001_order_enriched.sql)
  ~ customer_360            CHANGED    (002_customer_360.sql)
  = product_catalog         UNCHANGED  (003_product_catalog.sql)
  = promotion_usage         UNCHANGED  (004_promotion_usage.sql)
  = product_sales           UNCHANGED  (005_product_sales.sql)

Summary: 0 new, 1 changed, 4 unchanged, 0 removed
```

### Plan Before Experimenting

```bash
# 1. Edit a SQL file
vi sql/silver/002_customer_360.sql    # change LTV formula

# 2. Plan — see what changed
pg-warehouse run --plan --sql-dir ./sql/silver/

# 3. Apply to v2
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v2

# 4. Compare v1 vs v2 data
duckdb silver.duckdb "
SELECT 'v1' AS ver, COUNT(*) AS rows, ROUND(AVG(lifetime_revenue),2) AS avg_rev FROM v1.customer_360
UNION ALL
SELECT 'v2', COUNT(*), ROUND(AVG(lifetime_revenue),2) FROM v2.customer_360;
"

# 5. Promote v2 when satisfied
pg-warehouse run --promote --version 2
```

### Plan on Full Pipeline

```bash
# Plan what --pipeline will do
pg-warehouse run --plan --pipeline

# Output:
Plan: silver (v1 active → v1)
  = order_enriched          UNCHANGED
  = customer_360            UNCHANGED
  = product_catalog         UNCHANGED
  = promotion_usage         UNCHANGED
  = product_sales           UNCHANGED

Plan: feat (v1 → v1)
  = sales_summary           UNCHANGED
  = customer_analytics      UNCHANGED
  = product_performance     UNCHANGED
  = promotion_effectiveness UNCHANGED
  = inventory_health        UNCHANGED

Summary: 0 new, 0 changed, 10 unchanged, 0 removed
```

If everything is `UNCHANGED`, there's nothing to rebuild — the pipeline can skip execution entirely.

### How Checksums Are Tracked

Every time `--pipeline` or `--sql-dir` runs, pg-warehouse records a SHA256 checksum of each SQL file in `_meta.version_files`:

```sql
SELECT * FROM _meta.version_files;
```

```
version | filename                | checksum                         | target_table        | built_at
--------+-------------------------+----------------------------------+---------------------+---------------------
1       | 001_order_enriched.sql  | a3f2b1c4d5e6f7...               | v1.order_enriched   | 2026-03-22 18:14:10
1       | 002_customer_360.sql    | 7d4e9f0a1b2c3d...               | v1.customer_360     | 2026-03-22 18:14:16
```

The plan compares new file checksums against these stored values to detect changes without executing SQL.

---

## Versioned Development

### Create a New Version

```bash
# Auto-creates v2 schema + runs all silver SQL into v2
pg-warehouse run --sql-dir ./sql/silver/ --target-schema v2
```

The schema is auto-created. No need to run `silver create-version` first.

### Compare Versions

```bash
# In DuckDB CLI:
duckdb silver.duckdb

# Row counts
SELECT 'v1' AS ver, COUNT(*) FROM v1.order_enriched
UNION ALL
SELECT 'v2', COUNT(*) FROM v2.order_enriched;

# Aggregate drift
SELECT 'v1' AS ver, ROUND(AVG(lifetime_revenue),2) AS avg_rev FROM v1.customer_360
UNION ALL
SELECT 'v2', ROUND(AVG(lifetime_revenue),2) FROM v2.customer_360;
```

### Promote

```bash
# Swap current.* views to v2
pg-warehouse run --promote --version 2
```

This auto-registers v2 in `_meta.versions` and archives v1.

### Version History

```bash
duckdb silver.duckdb "SELECT * FROM _meta.versions;"
```

```
version | label | status   | promoted_at
--------+-------+----------+---------------------
1       | v1    | archived | 2026-03-22 18:14:42
2       | v2    | active   | 2026-03-22 19:30:15
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

1. Create `sql/silver/NNN_<table_name>.sql` (next numeric prefix)
2. Use `CREATE OR REPLACE TABLE <table_name> AS SELECT ...` (no schema prefix)
3. Reference source tables without schema prefixes (e.g., `FROM orders`, not `FROM v0.orders`)
4. Add header comment block (layer, target, description, sources)
5. Plan: `pg-warehouse run --plan --sql-dir ./sql/silver/`
6. Test: `pg-warehouse preview --sql-file ./sql/silver/NNN_<table_name>.sql --limit 10`
7. Run: `pg-warehouse run --sql-dir ./sql/silver/`

### New Feat Table

1. Create `sql/feat/NNN_<table_name>.sql` (next numeric prefix)
2. Use `CREATE OR REPLACE TABLE <table_name> AS SELECT ...` (no schema prefix)
3. Reference source tables without schema prefixes (e.g., `FROM order_enriched`)
4. Add header comment block
5. Plan: `pg-warehouse run --plan --sql-dir ./sql/feat/`
6. Test: `pg-warehouse preview --sql-file ./sql/feat/NNN_<table_name>.sql --limit 10`
7. Run: `pg-warehouse run --sql-dir ./sql/feat/`
8. Verify export: `ls -lh ./out/<table_name>.parquet`

### Verification

```bash
# Check run history
sqlite3 .pgwh/state.db 'SELECT * FROM feature_runs ORDER BY started_at DESC LIMIT 5;'

# Check what was built
duckdb silver.duckdb "SELECT * FROM _meta.version_files WHERE version = 1;"

# Verify Parquet output
ls -lh ./out/
```

---

## Command Reference

| Command | What it does |
|---------|-------------|
| `run --refresh` | Snapshot raw.duckdb → silver.duckdb v0 |
| `run --pipeline` | Run all sql/silver/*.sql + sql/feat/*.sql into v1 |
| `run --sql-dir DIR` | Run all SQL in DIR (sorted by numeric prefix) |
| `run --sql-dir DIR --target-schema v2` | Run SQL into v2 (auto-creates schema) |
| `run --sql-file FILE` | Run a single SQL file |
| `run --plan --sql-dir DIR` | Show plan: what would change vs current version |
| `run --plan --pipeline` | Show plan for full pipeline |
| `run --promote --version N` | Swap current.* views to vN |
| `run --refresh --pipeline --promote --version 1` | Production: do everything |
