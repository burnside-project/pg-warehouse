# pg-warehouse SPEC

## Overview

`pg-warehouse` is an open-core, local-first analytics platform written in Go.

The OSS version mirrors PostgreSQL tables into DuckDB, lets developers inspect synchronized data, and executes SQL feature jobs that export Parquet or CSV.

This repository uses **Hexagonal Architecture**.

## Product goals

### OSS goals
- connect to PostgreSQL
- mirror selected tables into DuckDB
- inspect mirrored data
- run SQL feature files
- export outputs to Parquet / CSV

### Paid goals
- CDC replication
- scheduling
- cloud exports
- remote metadata
- observability
- governance
- control plane

## Architectural style

The system uses:
- domain
- ports
- adapters
- services
- CLI

Infrastructure must not leak into domain logic.

Adapters must not call each other directly.

## OSS commands

- `init`
- `sync`
- `inspect tables`
- `inspect schema`
- `inspect sync-state`
- `run`
- `preview`
- `export`
- `doctor`

## DuckDB schemas

- `raw`
- `stage`
- `feat`
- `meta`

## Metadata tables

### `meta.sync_state`

| Column | Type |
|--------|------|
| table_name | TEXT PRIMARY KEY |
| sync_mode | TEXT |
| watermark_column | TEXT |
| last_watermark | TEXT |
| last_lsn | TEXT |
| last_snapshot_at | TIMESTAMP |
| last_sync_at | TIMESTAMP |
| last_status | TEXT |
| row_count | BIGINT |
| error_message | TEXT |

### `meta.sync_history`

| Column | Type |
|--------|------|
| run_id | TEXT |
| table_name | TEXT |
| sync_mode | TEXT |
| started_at | TIMESTAMP |
| finished_at | TIMESTAMP |
| inserted_rows | BIGINT |
| updated_rows | BIGINT |
| deleted_rows | BIGINT |
| status | TEXT |
| error_message | TEXT |

### `meta.feature_runs`

| Column | Type |
|--------|------|
| run_id | TEXT |
| sql_file | TEXT |
| target_table | TEXT |
| output_path | TEXT |
| output_type | TEXT |
| started_at | TIMESTAMP |
| finished_at | TIMESTAMP |
| row_count | BIGINT |
| status | TEXT |
| error_message | TEXT |

### `meta.feature_dependencies`

| Column | Type |
|--------|------|
| run_id | TEXT |
| source_table | TEXT |
| target_table | TEXT |

## SQL feature contract

For OSS v1, SQL files must create or replace a final table in `feat.*`.

Export should be handled by CLI, not by the SQL file itself.

## Open-core boundary

### OSS
- full snapshot sync
- watermark incremental sync
- local metadata store
- local exporters
- local DuckDB warehouse

### Paid
- CDC sync engine
- scheduler
- remote metadata
- cloud exporters
- control plane
- governance

## Testing

### Unit tests
- config parsing
- command argument handling
- metadata logic
- target table validation

### Integration tests
- Postgres seed
- full sync
- incremental sync
- feature run
- export validation
