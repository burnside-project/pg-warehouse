# Development Workflow: SQL Pipelines (raw → silver → feat)

This guide covers how to build SQL pipelines using pg-warehouse's Medallion Architecture. You'll learn the directory conventions, how to write silver and feat tables, the CDC pause/resume pattern, and how to run pipelines.

---

## Medallion Architecture

pg-warehouse uses a single DuckDB file with four schemas:

```
PostgreSQL (source)
   ↓ CDC / sync
DuckDB warehouse
   ├── raw.*    — Bronze: mirrored source tables (sync/CDC writes here)
   ├── stage.*  — Internal: merge buffer (auto-managed, not user-facing)
   ├── silver.* — Silver: curated, joined, enriched transforms
   └── feat.*   — Gold: analytics-ready aggregations + Parquet export
```

| Layer | Schema | Reads From | Written By | Purpose |
|-------|--------|------------|------------|---------|
| Bronze | `raw.*` | PostgreSQL | `sync`, `cdc start` | Exact mirror of source tables |
| Silver | `silver.*` | `raw.*` only | `pg-warehouse run` | Cleaned, joined, enriched data |
| Gold | `feat.*` | `silver.*` only | `pg-warehouse run` | Dashboard-ready aggregations |

**Strict layering rule:** Silver reads from `raw.*` only. Feat reads from `silver.*` only. This prevents tangled dependencies and makes each layer independently testable.

---

## Directory Conventions

```
sql/
├── silver/
│   ├── 001_order_enriched.sql
│   ├── 002_customer_360.sql
│   └── 003_product_catalog.sql
├── feat/
│   ├── 001_sales_summary.sql
│   ├── 002_customer_analytics.sql
│   └── 003_product_performance.sql
```

**Rules:**
- **Numeric prefix** (`001_`) = execution order. Files run in lexicographic sort order — no DAG needed.
- **One file = one table.** Filename matches the target table name (minus prefix).
- **Filepath determines target:** `sql/silver/001_order_enriched.sql` → `silver.order_enriched`
- Use `CREATE OR REPLACE TABLE` for idempotency — safe to re-run at any time.

---

## Writing a Silver Table

Silver tables clean, join, and enrich raw data. They read **only** from `raw.*`.

### Template

```sql
-- ============================================================================
-- Layer:       silver
-- Target:      silver.<table_name>
-- Description: <what this table represents>
-- Sources:     raw.<table1>, raw.<table2>, ...
-- ============================================================================

CREATE OR REPLACE TABLE silver.<table_name> AS
SELECT
    -- columns
FROM raw.<primary_table>
LEFT JOIN raw.<secondary_table>
    ON ...
```

### Walkthrough: `silver.order_enriched`

This table denormalizes orders by joining order items, payments, and shipments:

```sql
CREATE OR REPLACE TABLE silver.order_enriched AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.status AS order_status,
    COUNT(DISTINCT oi.id) AS line_item_count,
    SUM(oi.quantity * oi.unit_price) AS gross_amount,
    p.payment_method,
    s.shipped_at,
    s.delivered_at
FROM raw.orders o
LEFT JOIN raw.order_items oi ON o.id = oi.order_id
LEFT JOIN raw.payments p ON o.id = p.order_id
LEFT JOIN raw.shipments s ON o.id = s.order_id
GROUP BY o.id, o.customer_id, o.status, p.payment_method, s.shipped_at, s.delivered_at;
```

**Key patterns:**
- Aggregate child tables (order items) into the parent grain (one row per order)
- Use `COALESCE` for nullable aggregates
- Use `DISTINCT ON` subqueries for latest-record-per-key (e.g., latest payment)

---

## Writing a Feat Table

Feat tables produce analytics-ready outputs. They read **only** from `silver.*`.

### Template

```sql
-- ============================================================================
-- Layer:       feat
-- Target:      feat.<table_name>
-- Description: <what this table powers (dashboard, report, ML feature)>
-- Sources:     silver.<table1>, silver.<table2>, ...
-- ============================================================================

CREATE OR REPLACE TABLE feat.<table_name> AS
SELECT
    -- dimensions and metrics
FROM silver.<table>
GROUP BY ...
ORDER BY ...;
```

### Walkthrough: `feat.sales_summary`

Daily sales KPIs aggregated from enriched orders:

```sql
CREATE OR REPLACE TABLE feat.sales_summary AS
SELECT
    CAST(order_date AS DATE) AS sale_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(net_amount) AS net_revenue,
    ROUND(AVG(net_amount), 2) AS avg_order_value
FROM silver.order_enriched
GROUP BY CAST(order_date AS DATE)
ORDER BY sale_date;
```

**Key patterns:**
- Aggregate to the grain your dashboard needs (daily, weekly, per-customer, per-product)
- Use `CASE WHEN` for segmentation and status classification
- Use window functions (`RANK`, `ROW_NUMBER`) for rankings
- Every feat table gets exported to Parquet automatically by the pipeline

---

## The CDC Pause/Resume Pattern

DuckDB uses a single-writer lock. CDC holds a write connection while streaming, so SQL pipelines can't write concurrently. The solution: **stop CDC → run pipeline → restart CDC**.

### Why this is necessary

```
CDC running  →  DuckDB locked  →  pipeline `CREATE TABLE` fails
CDC stopped  →  DuckDB free    →  pipeline runs successfully
```

CDC is a long-running process. Stopping it for a few minutes to run pipelines is safe — the PostgreSQL replication slot retains all WAL changes, so nothing is lost. CDC catches up the delta when it restarts.

### How the orchestrator handles it

The `scripts/run-pipeline.sh` script automates this:

1. **Stop CDC** — sends `SIGINT` for graceful shutdown, waits up to 15s
2. **Clear stale lock** — removes the lock row from SQLite state
3. **Run silver SQL** — each file in order
4. **Run feat SQL** — each file in order, with Parquet export
5. **Restart CDC** — via `nohup` in background
6. **Trap handler** — if any step fails, CDC still restarts

```bash
# The trap ensures CDC always restarts, even on failure
trap cleanup EXIT
```

---

## Running the Pipeline

### Make Targets

```bash
make pipeline          # Full run: stop CDC → silver → feat → restart CDC
make pipeline-silver   # Silver layer only
make pipeline-feat     # Feat layer only
make pipeline-preview  # Preview feat SQL output (10 rows each, no writes)
make pipeline-status   # Show recent runs from SQLite state
```

### Manual Steps

If you prefer running individual files:

```bash
# 1. Stop CDC
kill -SIGINT $(pgrep -f 'pg-warehouse cdc')
sleep 5
sqlite3 .pgwh/state.db 'DELETE FROM lock_state;'

# 2. Run a silver table
pg-warehouse run \
    --sql-file ./sql/silver/001_order_enriched.sql \
    --target-table silver.order_enriched

# 3. Run a feat table with export
pg-warehouse run \
    --sql-file ./sql/feat/001_sales_summary.sql \
    --target-table feat.sales_summary \
    --output ./out/sales_summary.parquet \
    --file-type parquet

# 4. Restart CDC
nohup pg-warehouse cdc start --config pg-warehouse.yml > cdc.log 2>&1 &
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
2. Use `CREATE OR REPLACE TABLE silver.<table_name> AS SELECT ...`
3. Read only from `raw.*` tables
4. Add header comment block (layer, target, description, sources)
5. Test: `pg-warehouse preview --sql-file ./sql/silver/NNN_<table_name>.sql --limit 10`
6. Run: `pg-warehouse run --sql-file ./sql/silver/NNN_<table_name>.sql --target-table silver.<table_name>`

### New Feat Table

1. Create `sql/feat/NNN_<table_name>.sql` (next numeric prefix)
2. Use `CREATE OR REPLACE TABLE feat.<table_name> AS SELECT ...`
3. Read only from `silver.*` tables
4. Add header comment block
5. Test: `pg-warehouse preview --sql-file ./sql/feat/NNN_<table_name>.sql --limit 10`
6. Run: `pg-warehouse run --sql-file ./sql/feat/NNN_<table_name>.sql --target-table feat.<table_name> --output ./out/<table_name>.parquet`
7. Verify export: `ls -lh ./out/<table_name>.parquet`

### Verification

```bash
# Check run history
sqlite3 .pgwh/state.db 'SELECT * FROM feature_runs ORDER BY started_at DESC LIMIT 5;'

# Inspect the new table
pg-warehouse inspect schema silver.<table_name>
pg-warehouse inspect schema feat.<table_name>

# Verify Parquet output
ls -lh ./out/
```
