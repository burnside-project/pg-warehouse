# Silver Versioning

## Overview

Silver versioning lets you develop, test, and compare data transforms without affecting production dashboards. Each version is a DuckDB schema (v1, v2, v3...) inside `silver.duckdb`. A `current` schema contains views that point to the active production version.

## Why Version Silver?

Without versioning, changing a silver SQL file immediately affects all downstream feat tables and dashboards. If the change is wrong, dashboards show bad data until you fix and re-run.

With versioning:

```
v2 (production) ──────── current.* ──────── feat.* ──────── Dashboard
                                                               │
v3 (experiment) ──── compare with v2 ──── validate ──── promote if good
```

You develop v3 in isolation. Production dashboards keep reading v2 via `current.*`. When v3 is validated, promotion swaps the views instantly.

## Version Lifecycle

### Create

```bash
pg-warehouse silver create-version --label "add LTV formula"
# Created version v3 (schema: v3)
```

This creates a new DuckDB schema and registers it in `_meta.versions`.

### Build

Write SQL targeting the new version schema:

```sql
-- sql/silver/v3/002_customer_360.sql
CREATE OR REPLACE TABLE v3.customer_360 AS
SELECT
    ...
    -- New: improved LTV formula
    CASE
        WHEN customer_lifetime_days > 30 THEN
            ROUND(lifetime_revenue / (customer_lifetime_days / 30.0) * 12, 2)
        ELSE lifetime_revenue * 4  -- v3 change: assume quarterly repeat
    END AS estimated_annual_ltv
FROM wh.raw.customers c
...
```

Run the transform:

```bash
pg-warehouse run --sql-file sql/silver/v3/002_customer_360.sql \
    --target-table v3.customer_360
```

Tables that didn't change can inherit from the parent version (views to v2 tables).

### Compare

```bash
pg-warehouse silver compare --base v2 --candidate v3
```

Output:

```
┌─────────────────────┬──────────────┬──────────────┬──────────┐
│ Table               │ v2 rows      │ v3 rows      │ Delta    │
├─────────────────────┼──────────────┼──────────────┼──────────┤
│ customer_360        │ 1,000,000    │ 1,000,000    │ 0        │
│ order_enriched      │ 6,774,800    │ 6,774,800    │ 0 (view) │
│ product_catalog     │ 100,003      │ 100,003      │ 0 (view) │
└─────────────────────┴──────────────┴──────────────┴──────────┘

Schema changes in v3:
  ~ customer_360.estimated_annual_ltv  (formula changed)

Aggregate drift:
  customer_360.estimated_annual_ltv: AVG v2=$1,247.30 → v3=$1,891.55 (+51.6%)
  ⚠ Significant drift detected — review before promoting
```

You can also compare directly in SQL:

```sql
SELECT
    v2.customer_id,
    v2.estimated_annual_ltv AS v2_ltv,
    v3.estimated_annual_ltv AS v3_ltv,
    v3.estimated_annual_ltv - v2.estimated_annual_ltv AS diff
FROM v2.customer_360 v2
JOIN v3.customer_360 v3 ON v2.customer_id = v3.customer_id
WHERE ABS(v3.estimated_annual_ltv - v2.estimated_annual_ltv) > 100
ORDER BY ABS(diff) DESC
LIMIT 20;
```

### Promote

```bash
pg-warehouse silver promote --version 3
# Promoted v3 to production. current.* now points to v3.
```

What happens internally:

```sql
CREATE OR REPLACE VIEW current.customer_360 AS SELECT * FROM v3.customer_360;
CREATE OR REPLACE VIEW current.order_enriched AS SELECT * FROM v3.order_enriched;
CREATE OR REPLACE VIEW current.product_catalog AS SELECT * FROM v3.product_catalog;
```

Promotion is instant — no data copied. Feat SQL reads `current.*` and immediately sees v3 data on the next pipeline run.

### Rollback

```bash
pg-warehouse silver promote --version 2
# Rolled back to v2. current.* now points to v2.
```

Rollback is identical to promotion — just swap the view target.

### Archive / Drop

```bash
pg-warehouse silver drop-version --version 1
# Dropped v1 (status was 'archived'). Freed 200MB.
```

Only archived versions can be dropped. Active and experiment versions are protected.

## Version Registry

The `_meta.versions` table tracks all versions:

```
version │ label                    │ status     │ created_at       │ promoted_at
────────┼──────────────────────────┼────────────┼──────────────────┼──────────────
1       │ initial model            │ archived   │ 2026-03-15 10:00 │ 2026-03-15 10:05
2       │ added address fields     │ archived   │ 2026-03-18 14:00 │ 2026-03-18 14:30
3       │ improved LTV formula     │ active     │ 2026-03-20 09:00 │ 2026-03-20 11:00
4       │ testing new segmentation │ experiment │ 2026-03-20 14:00 │
```

## Feat SQL: Always Reads from current.*

Feat SQL files never reference a specific version. They always read from `current.*`:

```sql
-- sql/feat/001_sales_summary.sql
-- This SQL NEVER changes when silver versions change
CREATE OR REPLACE TABLE feat.sales_summary AS
SELECT
    CAST(order_date AS DATE) AS sale_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_total) AS gross_revenue,
    ...
FROM current.order_enriched
GROUP BY CAST(order_date AS DATE)
ORDER BY sale_date;
```

This decoupling means:
- Silver developers iterate freely without touching feat SQL
- Feat SQL is stable and predictable
- Dashboard queries are always valid

## Inheritance: Partial Version Changes

When creating v3, you often only change one or two silver tables. Unchanged tables inherit from the parent version via views:

```
v3 schema:
  v3.customer_360      ← real table (your new SQL)
  v3.order_enriched    ← VIEW → v2.order_enriched (inherited)
  v3.product_catalog   ← VIEW → v2.product_catalog (inherited)
```

This saves disk space and build time. Only rebuild what you changed.

## Epoch Pinning

Each version records which raw data epoch it was built from:

```
version 3: built from epoch 42
version 2: built from epoch 38
```

This means:
- v2 and v3 may be built from **different** raw data snapshots
- For a fair comparison, rebuild both from the same epoch
- The epoch is recorded in `_meta.versions` for audit/reproducibility

## Time Travel for Debugging

```bash
# "Sales numbers looked wrong yesterday at 2pm"
# Epoch 38 was committed around 2pm yesterday

pg-warehouse silver create-version --label "debug yesterday"
pg-warehouse run --epoch 38 --sql-file sql/silver/v4/001_order_enriched.sql \
    --target-table v4.order_enriched

# Now compare v4 (epoch 38) with v3 (epoch 42) to find the discrepancy
pg-warehouse silver compare --base v4 --candidate v3
```
