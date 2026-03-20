# Multi-DuckDB Architecture

## Overview

pg-warehouse supports two operating modes:

- **Single-file mode** (default): All schemas (raw, stage, silver, feat) live in one `warehouse.duckdb` file. Simple setup, but CDC must pause during pipeline runs due to DuckDB's single-writer lock.

- **Multi-file mode**: Three separate DuckDB files, each owned by a different process. CDC streams continuously while pipelines run independently. Zero downtime.

## The Problem: Single-Writer Lock

DuckDB enforces a single-writer lock per database file. In single-file mode:

```
CDC process           Pipeline process
     │                      │
     ▼                      ▼
INSERT INTO raw.*     CREATE TABLE silver.*
     │                      │
     └──── both write ──────┘
                │
                ▼
         warehouse.duckdb
         (BLOCKED — only one writer allowed)
```

CDC must be stopped before the pipeline runs, creating a downtime window where WAL events accumulate on PostgreSQL.

## The Solution: Three DuckDB Files

```
PostgreSQL
     │ WAL streaming (never stops)
     ▼
┌──────────────────────────────────────────┐
│  warehouse.duckdb  (BLACK BOX — CDC owns)│
│                                          │
│  CDC writes exclusively to this file.    │
│  Users cannot modify it.                 │
│                                          │
│  stage.*  → dedup/merge buffer           │
│  raw.*    → clean, deduped source data   │
│  _epochs  → epoch metadata               │
└──────────────────┬───────────────────────┘
                   │ ATTACH READ_ONLY
                   ▼
┌──────────────────────────────────────────┐
│  silver.duckdb  (DEVELOPMENT PLATFORM)   │
│                                          │
│  Users write SQL transforms here.        │
│  Versioned schemas for safe development. │
│                                          │
│  v1.*      → version 1 (archived)        │
│  v2.*      → version 2 (production)      │
│  current.* → views pointing to v2.*      │
│  _meta.*   → version registry            │
└──────────────────┬───────────────────────┘
                   │ ATTACH READ_ONLY
                   ▼
┌──────────────────────────────────────────┐
│  feature.duckdb                          │
│                                          │
│  Analytics-ready aggregations.           │
│  Reads from silver current.* only.       │
│                                          │
│  feat.*   → dashboard-ready tables       │
│           → Parquet export               │
│           → Dashboard + AI Q&A           │
└──────────────────────────────────────────┘
```

Each file has a single writer. No contention. CDC never stops.

## Configuration

### Single-File Mode (Legacy)

```yaml
duckdb:
  path: ./warehouse.duckdb
```

### Multi-File Mode

```yaml
duckdb:
  warehouse: ./warehouse.duckdb    # CDC black box
  silver: ./silver.duckdb          # development platform
  feature: ./feature.duckdb        # analytics output
```

## The Black Box: warehouse.duckdb

CDC owns this file exclusively. It contains:

| Schema | Purpose | Visible to Users |
|--------|---------|-----------------|
| `stage.*` | Append-only CDC merge buffer | No |
| `raw.*` | Clean, deduped source tables (one row per PK) | Read-only |
| `raw._epochs` | Epoch checkpoint metadata | Read-only |

### Epoch System

CDC stamps every row with an `_epoch` column. Epochs are committed at regular intervals (default: every 60 seconds or 10,000 rows).

```
CDC stream:  ═══════╤════════════╤════════════╤══════════►
                    │            │            │
                 epoch 40      epoch 41      epoch 42
                 (merged)      (merged)      (open)
```

An epoch lifecycle:

1. **Open**: CDC writes events to `stage.*` with `_epoch = N`
2. **Committed**: After N seconds/rows, epoch is sealed. LSN checkpoint recorded.
3. **Merged**: Stage rows are merged into `raw.*` (dedup by PK, last-write-wins). Stage rows deleted.

The pipeline only reads data from **merged epochs** — never from the open epoch. This guarantees:

- No partial writes visible
- All related tables consistent at the same point in time
- DELETE events handled via tombstones (`_deleted = true`)

### Dedup Flow

```
WAL event: UPDATE orders SET status='shipped' WHERE id=1001
     │
     ▼
stage.orders:  {id:1001, status:'shipped', _epoch:42, _deleted:false}
     │
     │  epoch 42 commits, merge runs:
     ▼
raw.orders:    {id:1001, status:'shipped'}  ← exactly one row, latest version
```

Users always see `raw.orders` with one row per PK. No duplicates. No version history in raw.

## The Development Platform: silver.duckdb

This is where users work. Silver reads from `warehouse.duckdb` via `ATTACH READ_ONLY` and writes curated, joined, enriched tables.

### How ATTACH Works

```sql
-- Inside silver.duckdb connection
ATTACH 'warehouse.duckdb' AS wh (READ_ONLY);

CREATE OR REPLACE TABLE v2.order_enriched AS
SELECT ...
FROM wh.raw.orders o
LEFT JOIN wh.raw.order_items oi ON o.id = oi.order_id
...
```

`ATTACH READ_ONLY` does not take a write lock. CDC continues writing to `warehouse.duckdb` while the pipeline reads a consistent MVCC snapshot.

### Versioned Schemas

Silver uses DuckDB schemas for versioning:

```
silver.duckdb
  ├── v1.*                  ← version 1 (archived)
  ├── v2.*                  ← version 2 (production)
  ├── v3.*                  ← version 3 (experiment)
  │
  ├── current.*             ← VIEWS pointing to active version
  │     ├── current.order_enriched    → SELECT * FROM v2.order_enriched
  │     ├── current.customer_360     → SELECT * FROM v2.customer_360
  │     └── current.product_catalog  → SELECT * FROM v2.product_catalog
  │
  └── _meta.versions        ← version registry table
```

### Version Lifecycle

```bash
# Create a new version
pg-warehouse silver create-version --label "add shipping address"

# Build silver tables for the new version
pg-warehouse run --sql-file sql/silver/v3/001_order_enriched.sql --target-table v3.order_enriched

# Compare with production
pg-warehouse silver compare --base v2 --candidate v3

# Promote to production (swaps current.* views)
pg-warehouse silver promote --version 3

# List all versions
pg-warehouse silver list-versions
```

Promotion is instant — it swaps `current.*` views, no data copied. Rollback is equally instant.

### SQL Directory Structure

```
sql/
├── silver/
│   ├── v1/001_order_enriched.sql       ← archived
│   ├── v2/001_order_enriched.sql       ← production
│   │   ├── 002_customer_360.sql
│   │   └── 003_product_catalog.sql
│   └── v3/001_order_enriched.sql       ← experiment (only changed files)
├── feat/
│   ├── 001_sales_summary.sql           ← always reads from current.*
│   └── 002_customer_analytics.sql
```

Feat SQL always reads from `current.*`. It never changes regardless of which version is active.

## The Analytics Layer: feature.duckdb

Feature reads from `silver.duckdb` via ATTACH and produces dashboard-ready aggregations.

```sql
-- Inside feature.duckdb connection
ATTACH 'silver.duckdb' AS silver (READ_ONLY);

CREATE OR REPLACE TABLE feat.sales_summary AS
SELECT ...
FROM silver.current.order_enriched
GROUP BY ...
```

Feature tables are exported to Parquet for the dashboard and AI Q&A.

## Data Contract at Each Boundary

| Boundary | Contract | Guarantee |
|----------|----------|-----------|
| PostgreSQL → warehouse.duckdb | WAL streaming via pglogrepl | All committed transactions replicated |
| warehouse.duckdb (raw.*) | Epoch-consistent, deduped | One row per PK, all tables at same epoch |
| warehouse.duckdb → silver.duckdb | ATTACH READ_ONLY | MVCC snapshot, no lock contention |
| silver.duckdb (current.*) | Views to active version | Stable interface for feat SQL |
| silver.duckdb → feature.duckdb | ATTACH READ_ONLY | Stable snapshot of current.* |
| feature.duckdb → Parquet | COPY TO export | Immutable files, safe for concurrent reads |

## Rebuild and Recovery

Each layer is rebuildable from the layer below:

```
PostgreSQL       → warehouse.duckdb  (re-sync from source)
warehouse.duckdb → silver.duckdb     (re-run silver SQL)
silver.duckdb    → feature.duckdb    (re-run feat SQL)
feature.duckdb   → Parquet           (re-export)
```

Only PostgreSQL is irreplaceable. Everything downstream can be reconstructed.

## Resource Budget (Typical)

| File | Size | Writer |
|------|------|--------|
| warehouse.duckdb | ~850MB (for 53M source rows) | CDC process |
| silver.duckdb | ~200-400MB (denormalized joins) | Pipeline process |
| feature.duckdb | ~50-100MB (aggregated) | Pipeline process |
| Parquet exports | ~100-200MB | Pipeline process |

## Migration from Single-File to Multi-File

1. Stop CDC
2. Change config from `duckdb.path` to `duckdb.warehouse` / `silver` / `feature`
3. Run `pg-warehouse init` to create silver.duckdb and feature.duckdb
4. Re-run silver and feat SQL to populate the new files
5. Restart CDC — it continues from the last confirmed LSN

Existing `warehouse.duckdb` keeps its raw.* data. No data migration needed.
