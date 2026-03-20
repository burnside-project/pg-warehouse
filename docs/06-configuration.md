# Configuration Reference

pg-warehouse is configured via a YAML file (default: `pg-warehouse.yml`).

## Full Configuration

```yaml
project:
  name: my_warehouse

postgres:
  url: postgres://user:pass@localhost:5432/mydb
  schema: public
  max_conns: 2
  connect_timeout: 5s
  query_timeout: 30s

duckdb:
  path: ./warehouse.duckdb

state:
  path: .pgwh/state.db

cdc:
  enabled: false
  publication_name: pgwh_pub
  slot_name: pgwh_slot
  tables:
    - public.orders
    - public.customers

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

## Section Reference

### project

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| name | string | — | Project name |

### postgres

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| url | string | **required** | PostgreSQL connection URL |
| schema | string | `public` | Default source schema |
| max_conns | int | `2` | Max pool connections (capped at 5) |
| connect_timeout | string | `5s` | Connection timeout |
| query_timeout | string | `30s` | Query timeout |

### duckdb

**Single-file mode** (default):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| path | string | **required** | Path to DuckDB file (all schemas in one file) |

**Multi-file mode** (zero-downtime, see [Multi-DuckDB Architecture](09-multi-duckdb-architecture.md)):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| warehouse | string | — | CDC black box: raw.* + stage.* (CDC-owned, exclusive writer) |
| silver | string | — | Development platform: versioned silver schemas (pipeline-owned) |
| feature | string | — | Analytics output: feat.* tables + Parquet export (pipeline-owned) |

Use either `path` (single-file) or `warehouse`/`silver`/`feature` (multi-file). If `warehouse` is set, multi-file mode is activated.

### state

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| path | string | `.pgwh/state.db` | Path to SQLite state database |

### cdc

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| enabled | bool | `false` | Enable CDC mode |
| publication_name | string | `pgwh_pub` | PostgreSQL publication name |
| slot_name | string | `pgwh_slot` | Replication slot name |
| tables | []string | — | Tables to replicate via CDC |
| epoch_interval_sec | int | `60` | Seconds between epoch commits (multi-file mode) |
| epoch_max_rows | int | `10000` | Max rows before forcing epoch commit (multi-file mode) |
| max_lag_bytes | int | `5368709120` (5GB) | Stop CDC if replication lag exceeds this. Prevents PostgreSQL disk fill. 0 = disabled. |
| drop_slot_on_exit | bool | `false` | Drop replication slot on CDC exit. Prevents orphaned WAL accumulation. |
| health_check_sec | int | `60` | Interval for replication lag health checks (seconds). |

### sync

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| mode | string | `incremental` | Default sync mode |
| default_batch_size | int | `50000` | Rows per batch |

### sync.tables[]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| name | string | **required** | Source table (schema.table) |
| target_schema | string | `raw` | DuckDB target schema |
| primary_key | []string | **required** | Primary key columns |
| watermark_column | string | — | Column for incremental sync |

### run

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| default_output_dir | string | `./out` | Default export directory |
| default_file_type | string | `parquet` | Default export format |

### logging

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| level | string | `info` | Log level (debug, info, warn, error) |
| format | string | `text` | Log format |

## CLI Flag

All commands accept `--config` to specify a custom config file:

```bash
pg-warehouse --config /path/to/config.yml sync
```

## Validation

The `doctor` command validates the configuration:

```bash
pg-warehouse doctor
```

Required fields: `postgres.url`, `duckdb.path`, at least one table in `sync.tables` with a `primary_key`.
