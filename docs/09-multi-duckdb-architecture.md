# Multi-DuckDB Architecture

## Overview

pg-warehouse uses three separate DuckDB files, each owned by a different process. CDC streams continuously while pipelines run independently. Zero downtime.

| File | Purpose | Owner |
|------|---------|-------|
| `raw.duckdb` | CDC black box. Deduped PostgreSQL mirror. | CDC process (exclusive) |
| `silver.duckdb` | Development platform. Curated transforms. | Developer / pipeline |
| `feature.duckdb` | Analytics output. Dashboard-ready aggregations. | Developer / pipeline |

## The Problem: Single-Writer Lock

DuckDB enforces a single-writer lock per database file. If CDC and the pipeline share a file, CDC must stop every time the pipeline runs.

The solution: **separate files**. CDC writes to `raw.duckdb`. The pipeline writes to `silver.duckdb` and `feature.duckdb`. No lock contention. CDC never stops.

## Architecture

```
PostgreSQL
     | WAL streaming (never stops)
     v
raw.duckdb (CDC BLACK BOX)
     |-- stage.*         append-only WAL events (CDC internal)
     |                     multiple versions per PK, tombstones
     |                     _epoch and _deleted columns
     |                       |
     |                       | merge (dedup by PK, every 60s)
     |                       v
     |-- raw.*           deduped source tables (one row per PK, latest version)
                           no _epoch, no _deleted — clean data
                |
                |  pg-warehouse run --refresh
                |  (snapshot raw.duckdb -> copy raw.* into v0.*)
                v
silver.duckdb (DEVELOPMENT PLATFORM)
     |-- v0.*            copy of raw.* (read-only, frozen between refreshes)
     |                     v0.orders, v0.customers, v0.order_items, ...
     |                       |
     |                       | pg-warehouse run --pipeline
     |                       | (001.sql, 002.sql, 003.sql... in numeric order)
     |                       v
     |-- v1.*            silver transforms (developer SQL, reads from v0.*)
     |                     v1.order_enriched, v1.customer_360, ...
     |                       |
     |                       | pg-warehouse run --promote
     |                       | (swap current.* views to v1.*)
     |                       v
     |-- current.*       views pointing to active version
     |-- _meta.*         versions, refresh log
                |
                |  pg-warehouse run --refresh (for feature)
                |  (copy silver current.* into feature v0.*)
                v
feature.duckdb (ANALYTICS OUTPUT)
     |-- v0.*            copy of silver current.* (read-only)
     |                     v0.order_enriched, v0.customer_360, ...
     |                       |
     |                       | pg-warehouse run --pipeline
     |                       | (001.sql, 002.sql... in numeric order)
     |                       v
     |-- v1.*            feature transforms (reads from v0.*)
     |                     v1.sales_summary, v1.customer_analytics, ...
     |                       |
     |                       | pg-warehouse run --promote
     |                       v
     |-- current.*       views pointing to active version
     |-- _meta.*         versions, refresh log
                |
                |  export
                v
           out/*.parquet -> Dashboard -> AI Q&A
```

## Reserved Names

| Name | Meaning | Mutable by User |
|------|---------|----------------|
| `raw.duckdb` | CDC-owned PostgreSQL mirror | Never |
| `silver.duckdb` | Silver development database | Yes (v1+, not v0) |
| `feature.duckdb` | Feature analytics database | Yes (v1+, not v0) |
| `v0` | Upstream data mirror (auto-populated by `--refresh`) | Never (read-only) |
| `v1`, `v2`... | User transform versions | Yes |
| `current` | Production pointer (views) | Via `--promote` only |
| `_meta` | Internal metadata (versions, refresh log) | Never |
| `stage` | CDC merge buffer (inside raw.duckdb) | Never |

## Configuration

```yaml
duckdb:
  raw: ./raw.duckdb              # CDC black box
  silver: ./silver.duckdb        # development platform
  feature: ./feature.duckdb      # analytics output
```

## The `run` Command

The `run` command handles everything — refresh, transforms, promotion, export.

```bash
# Refresh v0 from upstream (snapshot raw.duckdb -> silver v0)
pg-warehouse run --refresh

# Run a single transform
pg-warehouse run --sql-file sql/silver/v1/001_order_enriched.sql

# Run all transforms in numeric order
pg-warehouse run --pipeline

# Promote current version
pg-warehouse run --promote

# Production: do everything in one command
pg-warehouse run --refresh --pipeline --promote
```

### How `--refresh` Works

```
1. cp raw.duckdb /tmp/snapshot_$$.duckdb         (filesystem copy, no DuckDB lock)
2. ATTACH /tmp/snapshot AS upstream (READ_ONLY)   (works because snapshot has no writer)
3. For each table in upstream.raw.*:
     CREATE OR REPLACE TABLE v0.{table} AS
       SELECT * FROM upstream.raw.{table}         (full copy, truncate + replace)
4. Record epoch + timestamp in _meta.refresh_log
5. DETACH + rm snapshot
```

CDC keeps running throughout. The `cp` is a filesystem operation — it doesn't need a DuckDB lock.

**Refresh is always a full copy**, not incremental. DuckDB is columnar — full table scans are fast. 50M rows copies in ~45 seconds. Simpler, idempotent, no drift.

### How `--pipeline` Works

```
1. List all *.sql files in sql/silver/v1/ (or sql/feat/)
2. Sort by filename (numeric prefix determines order)
3. Run each sequentially:
     001_order_enriched.sql     -> v1.order_enriched
     002_customer_360.sql       -> v1.customer_360
     003_product_catalog.sql    -> v1.product_catalog
     004_promotion_usage.sql    -> v1.promotion_usage
     005_product_sales.sql      -> v1.product_sales
```

The numeric prefix IS the dependency order. If `005_product_sales.sql` reads from `v1.product_catalog`, it must run after `003`.

### How `--promote` Works

```sql
CREATE OR REPLACE VIEW current.order_enriched AS SELECT * FROM v1.order_enriched;
CREATE OR REPLACE VIEW current.customer_360 AS SELECT * FROM v1.customer_360;
-- ... for each table in v1.*
```

Instant. No data copied. Rollback = promote an older version.

## The v0 Pattern

Every DuckDB file (except raw.duckdb) has the same internal structure:

```
any.duckdb
  |-- v0.*        upstream data mirror (auto-populated, read-only)
  |-- v1.*        user transforms (mutable)
  |-- v2.*        next version (experiment)
  |-- current.*   views -> active version
  |-- _meta.*     versions, refresh log
```

### What v0 Contains

| File | v0 Contains | Source |
|------|------------|--------|
| `silver.duckdb` | Raw tables (14 tables from PostgreSQL) | `raw.duckdb` raw.* |
| `feature.duckdb` | Silver tables (5 curated tables) | `silver.duckdb` current.* |

v0 is always "what the upstream layer gave me." Developer SQL reads from v0 and writes to v1.

### Why v0 Exists

Without v0, silver SQL would need to ATTACH raw.duckdb — but CDC holds the write lock. v0 solves this by copying the data into silver.duckdb once (via `--refresh`), then silver SQL reads locally with no lock conflicts.

```
Developer sees:    SELECT * FROM v0.orders
What happened:     raw.duckdb raw.orders -> snapshot -> v0.orders (full copy)
CDC involvement:   None. CDC never knew.
```

## Dedup: How raw.* Stays Clean

CDC receives WAL events that may contain multiple versions of the same row:

```
stage.orders (append-only, CDC internal):
  id=1001, status='pending',   _epoch=40, _deleted=false
  id=1001, status='shipped',   _epoch=41, _deleted=false
  id=1001, status='delivered', _epoch=42, _deleted=false

Merge (every 60 seconds):
  1. Dedup within stage: keep latest per PK (by _epoch)
  2. Check tombstones: _deleted=true -> DELETE from raw
  3. Apply to raw: DELETE old + INSERT new
  4. Cleanup: DELETE processed stage rows

raw.orders (after merge):
  id=1001, status='delivered'    <- single row, latest state, no _epoch column
```

**raw.* does NOT have `_epoch` or `_deleted` columns.** Those exist only in stage. Raw is clean — it looks exactly like the PostgreSQL source table, just deduped and current.

## Epoch System

Epochs are CDC's internal transactional boundaries. They guarantee that when you snapshot raw.duckdb, the data is consistent.

```
CDC stream:  ════╤════════════╤════════════╤════════════╤═══>
                 |            |            |            |
              epoch 40      epoch 41      epoch 42      epoch 43
              (merged)      (merged)      (merged)      (open)
```

- **Merged epochs** = data is in raw.* (clean, deduped)
- **Open epoch** = data is in stage.* (not yet merged)

When `--refresh` snapshots raw.duckdb, it captures all merged epochs. The open epoch is still in stage (or WAL) and is not included. This means v0 always contains consistent, fully-committed data.

The epoch ID is recorded in `_meta.refresh_log` so you can trace: "This silver was built from epoch 42."

## Data Contracts

| Boundary | What Happens | Guarantee |
|----------|-------------|-----------|
| PostgreSQL -> raw.duckdb | CDC WAL streaming | All committed transactions replicated |
| raw.duckdb (stage -> raw) | Epoch merge (dedup) | One row per PK, consistent across tables |
| raw.duckdb -> silver.duckdb v0 | `--refresh` (snapshot + full copy) | v0 = raw at last merged epoch |
| silver.duckdb v0 -> v1 | `--pipeline` (SQL transforms) | v1 = f(v0), idempotent |
| silver.duckdb v1 -> current | `--promote` (view swap) | current = v1, instant |
| silver.duckdb -> feature.duckdb v0 | `--refresh` (full copy of current) | v0 = silver current |
| feature.duckdb v0 -> v1 | `--pipeline` (SQL transforms) | v1 = f(v0), idempotent |
| feature.duckdb -> Parquet | export | Immutable files |

## Developer Workflow

```bash
# Morning: get fresh data
pg-warehouse run --refresh

# All day: iterate on silver SQL
pg-warehouse run --sql-file sql/silver/v1/002_customer_360.sql
# tweak SQL, re-run, check results...
pg-warehouse preview --sql-file sql/silver/v1/002_customer_360.sql --limit 10

# Happy? Run the full pipeline and promote
pg-warehouse run --pipeline --promote

# Build features
pg-warehouse run --refresh --pipeline --promote   # (targeting feature)

# Validate
pg-warehouse doctor
```

## Production Pipeline

```bash
# Cron: every 6 hours
pg-warehouse run --refresh --pipeline --promote
```

One command. Refreshes v0, builds silver, promotes, refreshes feature v0, builds features, promotes, exports Parquet. CDC never stops.

## Rebuild and Recovery

Each layer is rebuildable from the layer below:

```
PostgreSQL        -> raw.duckdb        (re-sync via CDC)
raw.duckdb        -> silver.duckdb v0  (--refresh)
silver.duckdb v0  -> v1                (--pipeline)
silver.duckdb     -> feature.duckdb v0 (--refresh)
feature.duckdb v0 -> v1                (--pipeline)
feature.duckdb    -> Parquet           (export)
```

Only PostgreSQL is irreplaceable. Delete silver.duckdb? `--refresh --pipeline` rebuilds it in minutes. Delete feature.duckdb? Same.

## What the Developer Never Sees

```
CDC streaming              <- hidden
raw.duckdb internals       <- hidden (stage.*, epochs, merge logic)
Snapshot mechanics          <- hidden (cp, attach, detach, cleanup)
v0 population logic        <- hidden (just --refresh)
Epoch tracking             <- hidden (recorded in _meta automatically)
Lock management            <- hidden (pg-warehouse handles it)
```

Developer sees: `--refresh`, `--pipeline`, `--promote`. Three flags. That's the entire interface.
