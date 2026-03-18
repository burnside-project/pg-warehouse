# Quickstart

## Application Defaults

pg-warehouse ships with the following immutable and configurable defaults. These are compiled into the binary and applied automatically when a config value is not explicitly set.

### DuckDB Warehouse

| Default | Value | Source |
|---------|-------|--------|
| Database file | `warehouse.duckdb` | `cmd/pg-warehouse/init.go` (CLI flag default) |
| Database type | Single embedded file, three schemas | `internal/adapters/duckdb/bootstrap.go` |

The warehouse is a **single DuckDB database** containing three schemas, created at init:

| Schema | Purpose | Written By |
|--------|---------|------------|
| `raw.*` | Mirrored source tables — exact copies of PostgreSQL data | `sync`, `cdc start` |
| `stage.*` | Temporary staging for incremental merge — auto-created and dropped per sync cycle | `sync` (incremental mode) |
| `feat.*` | SQL feature pipeline outputs — results of user-defined SQL transformations | `run` |

### State Database (SQLite)

| Default | Value | Source |
|---------|-------|--------|
| State DB path | `.pgwh/state.db` | `internal/config/config.go` |
| Schema version | `1` | `internal/adapters/sqlitestate/schema.go` |

State is stored in SQLite, **not DuckDB**, so it survives warehouse rebuilds. It tracks:

| Table | Purpose |
|-------|---------|
| `project_identity` | Project metadata (singleton) |
| `sync_state` | Per-table sync watermarks and LSN |
| `sync_history` | Bounded sync run history |
| `cdc_state` | Per-table CDC replication state |
| `feature_runs` | Bounded feature execution history |
| `audit_log` | Platform audit trail |
| `watermarks` | Named progress checkpoints (e.g., `cdc_confirmed_lsn`) |
| `lock_state` | Concurrent execution prevention |
| `schema_version` | Migration tracking |

### Configuration Defaults

These values are applied when the corresponding config field is omitted. Defined in `internal/config/config.go`.

| Setting | Default Value | Notes |
|---------|---------------|-------|
| Config file name | `pg-warehouse.yml` | Expected in working directory |
| `postgres.schema` | `public` | Source schema to read from |
| `postgres.max_conns` | `2` | Connection pool size (capped at 5) |
| `postgres.connect_timeout` | `5s` | |
| `postgres.query_timeout` | `30s` | |
| `state.path` | `.pgwh/state.db` | |
| `cdc.publication_name` | `pgwh_pub` | PostgreSQL publication name |
| `cdc.slot_name` | `pgwh_slot` | PostgreSQL replication slot name |
| `sync.default_batch_size` | `50000` | Rows per sync batch |
| `sync.tables[].target_schema` | `raw` | DuckDB target schema per table |
| `run.default_output_dir` | `./out` | Export output directory |
| `run.default_file_type` | `parquet` | `parquet` or `csv` |
| `logging.level` | `info` | `debug`, `info`, `warn`, `error` |
| `logging.format` | `text` | `text` or `json` |

### Internal Constants

These are hardcoded in the application and not configurable via YAML.

| Constant | Value | Source | Purpose |
|----------|-------|--------|---------|
| DuckDB insert batch size | `1000` rows | `internal/adapters/duckdb/warehouse.go` | Rows per INSERT statement |
| CDC lock TTL | `24 hours` | `internal/services/cdc_service.go` | Lock expiry for crash recovery |
| CDC max reconnect retries | `10` | `internal/services/cdc_service.go` | Auto-reconnect attempts before giving up |
| CDC batch flush | `100 events` or `1 second` | `internal/adapters/postgres/cdc.go` | Batched apply to DuckDB |
| CDC LSN confirmation | Every `10 seconds` | `internal/adapters/postgres/cdc.go` | Progress confirmation to PostgreSQL |
| Max PostgreSQL connections | `5` (hard cap) | `internal/config/config.go` | `max_conns` is capped regardless of config |

## Before You Begin

Complete these steps before installing or running pg-warehouse.

### 1. PostgreSQL Requirements

Verify your PostgreSQL version and configuration:

```sql
-- Must be PostgreSQL 10 or higher
SELECT version();

-- Check WAL level (must be 'logical' for CDC)
SHOW wal_level;

-- Check replication slots and senders (need at least 1 each for pg-warehouse)
SHOW max_replication_slots;   -- recommended: 4
SHOW max_wal_senders;         -- recommended: 4
```

If `wal_level` is not `logical`, update `postgresql.conf` and **restart PostgreSQL**:

```ini
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

### 2. PostgreSQL User Setup

The connection user needs ownership of the tables it will replicate and the `REPLICATION` privilege:

```sql
-- Create a dedicated warehouse user
CREATE USER warehouse WITH PASSWORD 'your_password' REPLICATION;

-- Grant access to the source database
GRANT CONNECT ON DATABASE mydb TO warehouse;
GRANT USAGE ON SCHEMA public TO warehouse;

-- Grant table ownership (required for CDC publication creation)
ALTER TABLE public.orders OWNER TO warehouse;
ALTER TABLE public.customers OWNER TO warehouse;
-- Repeat for all tables you want to replicate
```

### 3. Validate Connectivity

From the machine where pg-warehouse will run, verify you can reach PostgreSQL:

```bash
psql postgres://warehouse:your_password@your-pg-host:5432/mydb -c "SELECT 1;"
```

### 4. System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Go | 1.25+ | 1.25+ |
| PostgreSQL | 10+ | 16+ |
| DuckDB | Embedded (no install) | — |
| Disk space | 2x source data size | 3x source data size |
| Memory | 512 MB | 2 GB+ for large snapshots |

## Build

```bash
go build -o pg-warehouse ./cmd/pg-warehouse/
```

## Initialize

### Configuration File Reference

Create `pg-warehouse.yml` in your working directory. Below is a full configuration with every parameter documented:

```yaml
# ──────────────────────────────────────────────────────────────────────
# Project
# ──────────────────────────────────────────────────────────────────────
project:
  name: soak_test_warehouse           # Project identifier, stored in state DB

# ──────────────────────────────────────────────────────────────────────
# PostgreSQL Source
# ──────────────────────────────────────────────────────────────────────
postgres:
  url: postgres://warehouse:pg_warehouse@10.29.29.211:5432/soak_test
                                       # Connection string (user:pass@host:port/db)
  schema: public                       # Source schema to read from (default: public)
  max_conns: 2                         # Connection pool size, 1-5 (default: 2, capped at 5)
  connect_timeout: 5s                  # Time to wait for a connection (default: 5s)
  query_timeout: 120s                  # Max query execution time (default: 30s)

# ──────────────────────────────────────────────────────────────────────
# DuckDB Warehouse
# ──────────────────────────────────────────────────────────────────────
# Single embedded database file containing three schemas:
#   raw.*   — mirrored source tables (written by sync and CDC)
#   stage.* — temporary staging area for incremental merge (auto-managed)
#   feat.*  — SQL feature pipeline outputs (written by 'run' command)
duckdb:
  path: ./warehouse.duckdb            # Path to DuckDB file (created on init)

# ──────────────────────────────────────────────────────────────────────
# State Database
# ──────────────────────────────────────────────────────────────────────
# SQLite database that tracks sync progress, CDC state, and audit history.
# Decoupled from DuckDB so state survives warehouse rebuilds.
state:
  path: .pgwh/state.db                # Path to SQLite state file (default: .pgwh/state.db)

# ──────────────────────────────────────────────────────────────────────
# CDC (Change Data Capture)
# ──────────────────────────────────────────────────────────────────────
# Streams real-time changes from PostgreSQL using logical replication.
# Requires wal_level=logical and REPLICATION privilege on the user.
cdc:
  enabled: true                        # Enable CDC streaming (default: false)
  publication_name: pgwh_pub           # PostgreSQL publication name (default: pgwh_pub)
  slot_name: pgwh_slot                 # Replication slot name (default: pgwh_slot)
  tables:                              # Tables to include in the publication
    - public.products

# ──────────────────────────────────────────────────────────────────────
# Sync
# ──────────────────────────────────────────────────────────────────────
# Batch sync from PostgreSQL to DuckDB. First run does a full snapshot;
# subsequent runs use watermark-based incremental sync.
sync:
  mode: incremental                    # full | incremental (default: incremental)
  default_batch_size: 50000            # Rows per batch (default: 50000)
  tables:
    - name: public.products            # Source table (schema.table)
      target_schema: raw               # DuckDB target schema (default: raw)
      primary_key: [id]                # Primary key column(s), required for merge/CDC
      watermark_column: updated_at     # Column for incremental sync (timestamp or serial)

# ──────────────────────────────────────────────────────────────────────
# Run (Feature Pipelines)
# ──────────────────────────────────────────────────────────────────────
run:
  default_output_dir: ./out            # Default export directory (default: ./out)
  default_file_type: parquet           # parquet | csv (default: parquet)

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────
logging:
  level: debug                         # debug | info | warn | error (default: info)
  format: text                         # text | json (default: text)
```

### Configuration Sections Explained

| Section | Purpose | Required |
|---------|---------|----------|
| `project` | Names the warehouse instance; stored in state DB for identification | Yes |
| `postgres` | PostgreSQL connection settings and pool tuning | Yes |
| `duckdb` | Path to the DuckDB warehouse file (single file, three schemas) | Yes |
| `state` | Path to the SQLite state DB that tracks sync/CDC progress | No (defaults to `.pgwh/state.db`) |
| `cdc` | CDC configuration: publication name, slot, and table list | No (disabled by default) |
| `sync` | Batch sync mode, batch size, and per-table mapping | Yes (if using sync) |
| `run` | Default output directory and file format for feature pipelines | No (has defaults) |
| `logging` | Log level and format | No (defaults to `info` / `text`) |

### Run Init

```bash
pg-warehouse init --duckdb ./warehouse.duckdb
```

This creates:

| Artifact | Path | Description |
|----------|------|-------------|
| DuckDB warehouse | `./warehouse.duckdb` | Single database file with `raw`, `stage`, `feat` schemas |
| State database | `.pgwh/state.db` | SQLite database tracking sync watermarks, CDC LSN positions, feature run history, audit log, and lock state |

### Validate Init

```bash
pg-warehouse doctor
```

Expected output confirms all checks pass:

```
config:      ok
postgres:    ok
duckdb:      ok
schema_raw:  ok
schema_stage: ok
schema_feat: ok
```

## Sync Data

```bash
pg-warehouse sync
```

Syncs all configured tables from PostgreSQL into DuckDB `raw.*` schema. First run does a full snapshot; subsequent runs use watermark-based incremental sync.

## CDC (Real-Time Streaming)

### Setup

Creates the PostgreSQL publication and replication slot:

```bash
pg-warehouse cdc setup
```

### Start

Begins streaming changes in real-time:

```bash
pg-warehouse cdc start
```

Lifecycle: initial snapshot for new tables, then continuous WAL streaming with batched apply to DuckDB.

### Status

```bash
pg-warehouse cdc status
```

### Teardown

```bash
pg-warehouse cdc teardown
```

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
