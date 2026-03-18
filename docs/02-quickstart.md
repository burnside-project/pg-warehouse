# Quickstart

## Prerequisites

- Go 1.25+
- PostgreSQL 10+ with `wal_level=logical` (for CDC)
- PostgreSQL user with `REPLICATION` privilege and table ownership


### System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Go | 1.25+ | 1.25+ |
| PostgreSQL | 10+ | 16+ |
| DuckDB | Embedded (no install) | — |
| Disk space | 2x source data size | 3x source data size |
| Memory | 512 MB | 2 GB+ for large snapshots |


### PostgreSQL Configuration

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

### PostgreSQL User Setup

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

### Validate Connectivity

```bash
psql postgres://warehouse:your_password@your-pg-host:5432/mydb -c "SELECT 1;"
```
---

### Pre-Seeding DuckDB (Fast Initial Load)

For large databases, the default CDC snapshot can take hours because it loads each table row-by-row through the application. A faster approach uses the same pattern as production database replication: **capture a WAL position, bulk copy the data, then start replication from that position**.

This reduces initial seeding from hours to minutes (50 million rows in ~5-10 minutes vs. ~12 hours).

### Step 1: Setup CDC and Capture LSN

```bash
pg-warehouse cdc setup --config pg-warehouse.yml

# Capture the current WAL position — write this down
psql postgres://warehouse:password@pg-host:5432/mydb -tA \
  -c "SELECT pg_current_wal_lsn();"
# Output: 72/F1E38898
```

### Step 2 Create `pg-warehouse.yml` in your working directory. 

See the [Configuration File Reference](#configuration-file-reference) for all available parameters and their defaults.

Minimal example:

```yaml
project:
  name: my_warehouse

postgres:
  url: postgres://warehouse:password@pg-host:5432/mydb
  schema: public

duckdb:
  path: ./warehouse.duckdb

sync:
  mode: incremental
  tables:
    - name: public.orders
      target_schema: raw
      primary_key: [id]
      watermark_column: updated_at
    - name: public.customers
      target_schema: raw
      primary_key: [id]
      watermark_column: updated_at
```

### Step 3: Initialize DuckDB

```bash
pg-warehouse init --config pg-warehouse.yml
```

### Step 4: Bulk Load Tables

Use `pg_dump` to export and load into DuckDB. This is production-safe — `pg_dump` uses a `REPEATABLE READ` snapshot internally, respects connection limits, and is well understood by DBAs.

```bash
# Export (parallel, consistent snapshot)
pg_dump \
  --host=pg-host --port=5432 --username=warehouse --dbname=mydb \
  --format=directory --jobs=4 --data-only \
  --table='public.orders' --table='public.customers' \
  --file=/tmp/pg_dump_out
```

Alternatively, use `COPY TO` for more control:

```bash
psql postgres://warehouse:password@pg-host:5432/mydb <<'SQL'
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
COPY public.orders TO '/tmp/orders.csv' WITH (FORMAT csv, HEADER);
COPY public.customers TO '/tmp/customers.csv' WITH (FORMAT csv, HEADER);
COMMIT;
SQL
```

> New to these requirements? See [Prerequisites Details](#prerequisites-details) in the Reference section.

## 1. Building Binary

```bash
go build -o pg-warehouse ./cmd/pg-warehouse/
```

## 2. Configure

Create `pg-warehouse.yml` in your working directory. See the [Configuration File Reference](#configuration-file-reference) for all available parameters and their defaults.

Minimal example:

```yaml
project:
  name: my_warehouse

postgres:
  url: postgres://warehouse:password@pg-host:5432/mydb
  schema: public

duckdb:
  path: ./warehouse.duckdb

sync:
  mode: incremental
  tables:
    - name: public.orders
      target_schema: raw
      primary_key: [id]
      watermark_column: updated_at
    - name: public.customers
      target_schema: raw
      primary_key: [id]
      watermark_column: updated_at
```

## 3. Initialize

```bash
pg-warehouse init --duckdb ./warehouse.duckdb
```

Creates `warehouse.duckdb` (with `raw`, `stage`, `feat` schemas) and `.pgwh/state.db` (SQLite state).

## 4. Validate

```bash
pg-warehouse doctor
```

## 5. Sync Data

```bash
pg-warehouse sync
```

First run does a full snapshot. Subsequent runs use watermark-based incremental sync.

## 6. CDC (Real-Time Streaming)

```bash
# Create publication and replication slot
pg-warehouse cdc setup

# Start streaming (Ctrl+C to stop)
pg-warehouse cdc start

# Check status
pg-warehouse cdc status

# Remove publication and slot
pg-warehouse cdc teardown
```

## 7. Inspect

```bash
pg-warehouse inspect tables             # List all warehouse tables
pg-warehouse inspect schema raw.orders   # Describe a table
pg-warehouse inspect sync-state          # Show sync state
```

## 8. Run Feature SQL

```bash
pg-warehouse run \
  --sql-file ./sql/customer_features.sql \
  --target-table feat.customer_features \
  --output ./out/customer_features.parquet \
  --file-type parquet
```

## 9. Preview and Export

```bash
# Preview query results
pg-warehouse preview --sql-file ./sql/query.sql --limit 10

# Export a table
pg-warehouse export \
  --table feat.customer_features \
  --output ./out/export.csv \
  --file-type csv
```

---



Load into DuckDB:

```bash
duckdb warehouse.duckdb <<'SQL'
CREATE OR REPLACE TABLE raw.orders AS
  SELECT * FROM read_csv('/tmp/orders.csv', auto_detect=true);
CREATE OR REPLACE TABLE raw.customers AS
  SELECT * FROM read_csv('/tmp/customers.csv', auto_detect=true);
SQL
```

### Step 4: Start CDC from Captured LSN

```bash
pg-warehouse cdc start --from-lsn "72/F1E38898"
```

This skips the initial snapshot, sets all tables to the captured LSN, and starts WAL streaming. CDC catches up the small delta accumulated during the bulk copy (typically seconds).

### Performance Comparison

| Method | 50M rows / 9.6 GB | Production Safe |
|--------|-------------------:|:---------------:|
| Default CDC snapshot | ~12 hours | Yes |
| `pg_dump` + `--from-lsn` | **~5-10 minutes** | **Yes** |

> See [Pre-Seeding Details](#pre-seeding-details) in the Reference section for consistency notes and alternative methods.

---

# Reference



## Configuration File Reference

Full `pg-warehouse.yml` with every parameter documented. See [Configuration Defaults](#configuration-defaults) for values applied when a field is omitted.

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

### Configuration Sections

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

## Pre-Seeding Details

### Consistency Notes

- **`pg_dump`** uses a `REPEATABLE READ` snapshot internally, so all exported tables are consistent at the same point in time. This is the same mechanism PostgreSQL uses for backups and is safe for production workloads.
- **`COPY TO` in a `REPEATABLE READ` transaction** provides the same consistency guarantee with more flexibility (e.g., exporting to different formats, filtering columns).
- **The replication slot** created during `cdc setup` guarantees PostgreSQL retains all WAL from the moment it was created. No changes are lost between the LSN capture and the start of CDC streaming.
- **The WAL delta** between the captured LSN and when CDC starts is typically small (seconds to minutes of changes). CDC catches up this delta automatically before transitioning to live streaming.

### Alternative: DuckDB postgres_scan (non-production only)

For non-production databases or read replicas, DuckDB's `postgres_scan` extension is faster (~1 minute for 50M rows) but opens direct connections that can put significant load on the source:

```bash
duckdb warehouse.duckdb <<'SQL'
INSTALL postgres;
LOAD postgres;
CREATE OR REPLACE TABLE raw.orders AS
  SELECT * FROM postgres_scan(
    'dbname=mydb host=pg-host port=5432 user=warehouse password=password',
    'public', 'orders');
SQL
```

> **Do not use `postgres_scan` against production databases.** It performs full table scans without throttling or snapshot isolation. Use `pg_dump` instead.

## DuckDB Warehouse Architecture

The warehouse is a **single DuckDB database file** containing three schemas:

| Schema | Purpose | Written By |
|--------|---------|------------|
| `raw.*` | Mirrored source tables — exact copies of PostgreSQL data | `sync`, `cdc start` |
| `stage.*` | Temporary staging for incremental merge — auto-created and dropped per sync cycle | `sync` (incremental mode) |
| `feat.*` | SQL feature pipeline outputs — results of user-defined SQL transformations | `run` |

## State Database (SQLite)

State is stored in SQLite (`.pgwh/state.db`), **not DuckDB**, so it survives warehouse rebuilds.

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

## Configuration Defaults

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

## Internal Constants

Hardcoded in the application, not configurable via YAML.

| Constant | Value | Source | Purpose |
|----------|-------|--------|---------|
| DuckDB insert batch size | `1000` rows | `internal/adapters/duckdb/warehouse.go` | Rows per INSERT statement |
| CDC lock TTL | `24 hours` | `internal/services/cdc_service.go` | Lock expiry for crash recovery |
| CDC max reconnect retries | `10` | `internal/services/cdc_service.go` | Auto-reconnect attempts before giving up |
| CDC batch flush | `100 events` or `1 second` | `internal/adapters/postgres/cdc.go` | Batched apply to DuckDB |
| CDC LSN confirmation | Every `10 seconds` | `internal/adapters/postgres/cdc.go` | Progress confirmation to PostgreSQL |
| Max PostgreSQL connections | `5` (hard cap) | `internal/config/config.go` | `max_conns` is capped regardless of config |
