# Silver Versioning

## Overview

Silver versioning lets you develop, test, and compare data transforms without affecting production dashboards. Each version is a DuckDB schema (v1, v2, v3...) inside `silver.duckdb`. A `current` schema contains views that point to the active production version.

The `v0` schema holds a read-only copy of raw data from `raw.duckdb`, populated by `--refresh`. All silver SQL reads from `v0.*` and writes to a versioned schema (`v1.*`, `v2.*`, etc.).

## Schema Structure

```
silver.duckdb
  |-- v0.*            Raw data mirror (auto-populated by --refresh, read-only)
  |     |-- v0.orders
  |     |-- v0.customers
  |     |-- v0.order_items
  |     |-- ... (all 14 raw tables)
  |
  |-- v1.*            Silver transforms (your SQL, reads from v0.*)
  |     |-- v1.order_enriched
  |     |-- v1.customer_360
  |     |-- v1.product_catalog
  |     |-- v1.promotion_usage
  |     |-- v1.product_sales
  |
  |-- v2.*            Next version (experiment, reads from v0.*)
  |
  |-- current.*       Views -> active version (e.g., v1.*)
  |     |-- current.order_enriched  ->  SELECT * FROM v1.order_enriched
  |     |-- current.customer_360    ->  SELECT * FROM v1.customer_360
  |     |-- ...
  |
  |-- _meta.*
        |-- versions        version registry
        |-- refresh_log     when v0 was last refreshed, from which epoch
```

## The v0 Schema

v0 is the bridge between the CDC world and the development world. It contains a full copy of every raw table from `raw.duckdb`, populated by `pg-warehouse run --refresh`.

- **Read-only**: Developer SQL reads from v0, never writes to it
- **Frozen between refreshes**: v0 doesn't change while you iterate on SQL
- **Full replacement**: `--refresh` does truncate + full copy, not incremental
- **No CDC involvement**: CDC never touches silver.duckdb

```bash
# Populate v0 with latest raw data (takes ~45 seconds for 50M rows)
pg-warehouse run --refresh

# Now v0 has fresh data. Iterate on silver SQL all day.
# v0 won't change until you --refresh again.
```

## Developer Workflow

```bash
# 1. Get fresh data (once per session or when you want latest)
pg-warehouse run --refresh

# 2. Develop silver SQL — reads from v0.*, writes to v1.*
pg-warehouse run --sql-file sql/silver/v1/001_order_enriched.sql
# tweak SQL, re-run, check...
pg-warehouse run --sql-file sql/silver/v1/001_order_enriched.sql
# preview results
pg-warehouse preview --sql-file sql/silver/v1/001_order_enriched.sql --limit 10

# 3. Run full pipeline (all silver SQL in numeric order)
pg-warehouse run --pipeline

# 4. Promote to production
pg-warehouse run --promote

# 5. Validate
pg-warehouse doctor
```

## SQL Convention

Silver SQL files read from `v0.*` and write to a versioned schema:

```sql
-- sql/silver/v1/001_order_enriched.sql
CREATE OR REPLACE TABLE v1.order_enriched AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.total AS order_total,
    ...
FROM v0.orders o
LEFT JOIN v0.order_items oi ON o.id = oi.order_id
LEFT JOIN v0.payments p ON o.id = p.order_id
...
```

**No ATTACH needed.** Everything is inside silver.duckdb. Just `v0.orders`, not `wh.raw.orders`.

## Numeric File Ordering

SQL files use numeric prefixes to determine execution order:

```
sql/silver/v1/
  |-- 001_order_enriched.sql        <- runs first
  |-- 002_customer_360.sql          <- runs second
  |-- 003_product_catalog.sql       <- runs third
  |-- 004_promotion_usage.sql       <- runs fourth
  |-- 005_product_sales.sql         <- runs fifth (depends on 003)
```

`--pipeline` sorts by filename and runs sequentially. The number IS the dependency order.

## Version Lifecycle

### Create

```bash
pg-warehouse silver create-version --label "add LTV formula"
# Created version v2 (schema: v2)
```

### Build

```bash
# Run all transforms for v2
pg-warehouse run --pipeline --version 2

# Or run individual files
pg-warehouse run --sql-file sql/silver/v2/002_customer_360.sql
```

### Compare

```bash
pg-warehouse silver compare --base v1 --candidate v2
```

```
Table               | v1 rows    | v2 rows    | Delta
--------------------+------------+------------+--------
order_enriched      | 6,764,334  | 6,764,334  | 0
customer_360        | 1,000,000  | 1,000,000  | 0 (formula changed)
product_catalog     | 100,003    | 100,003    | 0

Aggregate drift:
  customer_360.estimated_annual_ltv: AVG v1=$1,247.30 -> v2=$1,891.55 (+51.6%)
```

### Promote

```bash
pg-warehouse run --promote --version 2
# current.* now points to v2.*
```

Instant — swaps views, no data copied.

### Rollback

```bash
pg-warehouse run --promote --version 1
# current.* now points back to v1.*
```

### Drop

```bash
pg-warehouse silver drop-version --version 1
# Dropped v1 (must be archived status)
```

## Version Registry

```sql
SELECT * FROM _meta.versions;
```

```
version | label                    | status     | epoch | created_at        | promoted_at
--------+--------------------------+------------+-------+-------------------+-----------------
1       | initial model            | archived   | 42    | 2026-03-20 11:00  | 2026-03-20 11:05
2       | improved LTV formula     | active     | 48    | 2026-03-21 09:00  | 2026-03-21 09:30
3       | testing new segmentation | experiment | 48    | 2026-03-21 14:00  |
```

## Refresh Log

```sql
SELECT * FROM _meta.refresh_log;
```

```
id | refreshed_at         | source     | epoch | tables | total_rows  | duration_ms
---+----------------------+------------+-------+--------+-------------+------------
1  | 2026-03-20 11:00:00  | raw.duckdb | 42    | 14     | 50,210,076  | 45,230
2  | 2026-03-21 09:00:00  | raw.duckdb | 48    | 14     | 51,430,221  | 47,110
```

## Feat SQL: Always Reads from v0.*

Feature SQL follows the same pattern. Feature v0 is populated from silver `current.*`:

```sql
-- sql/feat/001_sales_summary.sql
CREATE OR REPLACE TABLE v1.sales_summary AS
SELECT
    CAST(order_date AS DATE) AS sale_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_total) AS gross_revenue,
    ...
FROM v0.order_enriched
GROUP BY CAST(order_date AS DATE)
ORDER BY sale_date;
```

Feature SQL reads from `v0.*` (which contains silver `current.*` tables). It never references silver versions directly.

## Production Pipeline

```bash
# One command does everything
pg-warehouse run --refresh --pipeline --promote
```

What happens:
1. Snapshot raw.duckdb -> populate silver v0 (14 tables)
2. Run sql/silver/v1/*.sql in numeric order -> populate v1
3. Promote v1 -> swap current.* views
4. Copy silver current.* -> populate feature v0 (5 tables)
5. Run sql/feat/*.sql in numeric order -> populate feature v1
6. Promote feature v1 -> swap current.* views
7. Export to out/*.parquet
