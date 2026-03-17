# Quickstart

## Prerequisites

- Go 1.25+
- PostgreSQL 10+ (for sync) or PostgreSQL 10+ with `wal_level=logical` (for CDC)
- DuckDB (embedded, no separate install needed)

## Build

```bash
go build -o pg-warehouse ./cmd/pg-warehouse/
```

## Initialize

Create a config file `pg-warehouse.yml`:

```yaml
project:
  name: my_warehouse

postgres:
  url: postgres://user:pass@localhost:5432/mydb
  schema: public

duckdb:
  path: ./warehouse.duckdb

sync:
  mode: incremental
  default_batch_size: 50000
  tables:
    - name: public.orders
      target_schema: raw
      primary_key: [id]
      watermark_column: updated_at
    - name: public.customers
      target_schema: raw
      primary_key: [id]
      watermark_column: updated_at

run:
  default_output_dir: ./out
  default_file_type: parquet

logging:
  level: info
  format: text
```

Initialize the project:

```bash
pg-warehouse init --duckdb ./warehouse.duckdb
```

This creates:
- `warehouse.duckdb` — DuckDB warehouse with `raw`, `stage`, `feat` schemas
- `.pgwh/state.db` — SQLite state database

## Sync Data

```bash
pg-warehouse sync
```

Syncs all configured tables from PostgreSQL into DuckDB `raw.*` schema. First run does a full snapshot; subsequent runs use watermark-based incremental sync.

## Inspect

```bash
# List all warehouse tables
pg-warehouse inspect tables

# Describe a table's schema
pg-warehouse inspect schema raw.orders

# Show sync state
pg-warehouse inspect sync-state
```

## Run Feature SQL

```bash
pg-warehouse run \
  --sql-file ./sql/customer_features.sql \
  --target-table feat.customer_features \
  --output ./out/customer_features.parquet \
  --file-type parquet
```

## Preview

```bash
pg-warehouse preview --sql-file ./sql/query.sql --limit 10
```

## Export

```bash
pg-warehouse export \
  --table feat.customer_features \
  --output ./out/export.csv \
  --file-type csv
```

## Doctor

```bash
pg-warehouse doctor
```

Validates configuration, PostgreSQL connectivity, DuckDB accessibility, and state DB health.
