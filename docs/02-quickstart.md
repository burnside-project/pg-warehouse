# Quickstart

## System Requirements

- Go 1.25+
- PostgreSQL 10+ with `wal_level=logical` (for CDC)
- PostgreSQL user with `REPLICATION` privilege and table ownership

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Go | 1.25+ | 1.25+ |
| PostgreSQL | 10+ | 16+ |
| DuckDB | Embedded (no install) | — |
| Disk space | 2x source data size | 3x source data size |
| Memory | 512 MB | 2 GB+ for large snapshots |

---

## Before you Start - Prerequisites

### 1. Check PostgreSQL Configurations

```sql
-- Must be PostgreSQL 10 or higher
SELECT version();

-- Check WAL level (must be 'logical' for CDC)
SHOW wal_level;

-- Check replication slots and senders (need at least 1 each for pg-warehouse)
SHOW max_replication_slots;   -- recommended: 4
SHOW max_wal_senders;         -- recommended: 4
```

> If `wal_level` is not `logical`, update `postgresql.conf` and **restart PostgreSQL**:

```ini
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

### 2. PostgreSQL User Setup

> The connection user needs ownership of the tables it will replicate and the `REPLICATION` privilege:

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

```sql
-- We need to enable the replication user
SHOW hba_file;
```

> Will show location of the file like ... "/etc/postgresql/18/main/pg_hba.conf"

```bash
## Edit pg_hba.conf
# add following values at the bottom of the file
vi pg_hba.conf
```

```ini
# TYPE  DATABASE        USER            ADDRESS          METHOD
host    replication    warehouse    xx.xx.xx.0/24    scram-sha-256
# database user you created above | subnet where you are running pg-warehouse from
```

```sql
-- validate if your replication user warehouse has proper credentials
SELECT rolname, rolcanlogin, rolreplication
FROM pg_roles
WHERE rolname IN ('warehouse')

-- you will see something like this
-- rolcanlogin = true
-- rolreplication = true
```

> Validate Connectivity

```bash
psql postgres://warehouse:your_password@your-pg-host:5432/mydb -c "SELECT 1;"
```

### 3. Setup CDC and Get PostgreSQL LSN Number for Pre-Seeding DuckDB

> This is IMPORTANT because it creates a fast provision process!

The default CDC snapshot can take hours because it loads each table row-by-row from the database. A faster approach uses the same pattern as production database replication process: **capture a WAL position, bulk copy the data, then start replication from that position**.

This reduces initial seeding from hours to minutes (50 million rows in ~5-10 minutes vs. ~12 hours).

#### Performance Comparison

| Method | 50M rows / 9.6 GB | Production Safe |
|--------|-------------------:|:---------------:|
| Default CDC snapshot | ~12 hours | Yes |
| `COPY TO CSV` + `--from-lsn` | **~5-10 minutes** | **Yes** |

> See [Pre-Seeding Details](#pre-seeding-details) in the Reference section for consistency notes and alternative methods.

```bash
# Step A: Create the publication and replication slot FIRST
# This ensures PostgreSQL retains all WAL from this point forward
pg-warehouse cdc setup --config pg-warehouse.yml

# Step B: Capture the current WAL position — write this down
psql postgres://warehouse:password@pg-host:5432/mydb -tA \
  -c "SELECT pg_current_wal_lsn();"
# Example Output: 72/F1E38898
```

> **Important:** Write down the WAL position — you will need this in Step 3 below. The replication slot created in Step A guarantees no changes are lost between now and when CDC starts.

---

### 4. Create `pg-warehouse.yml` in your working directory.

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

cdc:
  enabled: true
  publication_name: pgwh_pub
  slot_name: pgwh_slot
  tables:
    - public.orders
    - public.customers

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

---

## Start pg-warehouse - The local-first Data Warehouse

### Step 1. Initialize DuckDB

```bash
pg-warehouse init --config pg-warehouse.yml
```

Creates `warehouse.duckdb` (with `raw`, `stage`, `silver`, `feat` schemas) and `.pgwh/state.db` (SQLite state).

### Step 2. Export Data from PostgreSQL and Load into DuckDB

Use PostgreSQL's `COPY TO` command to export tables as CSV, then load them into DuckDB with `read_csv`. This is production-safe — wrapping the exports in a `REPEATABLE READ` transaction guarantees all tables are exported at the exact same point in time (consistent snapshot).

> **Why not `pg_dump`?** `pg_dump --format=directory` produces PostgreSQL's internal binary
> format (`.dat` files) that only `pg_restore` can read — and `pg_restore` can only load into
> PostgreSQL, not DuckDB. `COPY TO CSV` produces standard CSV files that DuckDB reads natively.

#### Export tables as CSV (consistent snapshot)

```bash
psql postgres://warehouse:password@pg-host:5432/mydb <<'SQL'
-- REPEATABLE READ freezes the snapshot — all tables exported at the same point in time
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
COPY public.orders TO '/tmp/orders.csv' WITH (FORMAT csv, HEADER);
COPY public.customers TO '/tmp/customers.csv' WITH (FORMAT csv, HEADER);
-- Repeat for all tables in your CDC configuration
COMMIT;
SQL
```

#### Load into DuckDB

```bash
duckdb warehouse.duckdb <<'SQL'
CREATE OR REPLACE TABLE raw.orders AS
  SELECT * FROM read_csv('/tmp/orders.csv', auto_detect=true);
CREATE OR REPLACE TABLE raw.customers AS
  SELECT * FROM read_csv('/tmp/customers.csv', auto_detect=true);
SQL
```

### Step 3. Start CDC from Captured LSN

Use the LSN you captured in Prerequisites Step 3:

```bash
pg-warehouse cdc start --from-lsn "72/F1E38898"
```

> **`--from-lsn` is for first-time setup only.** It skips the initial snapshot and sets all tables to the provided LSN. On subsequent restarts (after crashes, upgrades, or reboots), do NOT use `--from-lsn` — just run `pg-warehouse cdc start` and it will resume from the last confirmed LSN in the state database. See [Upgrading pg-warehouse](#upgrading-pg-warehouse) below.

---

## Upgrading pg-warehouse

When deploying a new version of pg-warehouse, follow this procedure. CDC state is preserved in SQLite — no teardown, no re-seeding, no data loss.

> **Do NOT use `--from-lsn` on upgrades.** It resets the LSN backward and causes unnecessary WAL replay. The state database already has the correct LSN.

```bash
# 1. Stop CDC gracefully (saves final state to SQLite)
kill -SIGINT $(pgrep -f 'pg-warehouse cdc') && sleep 5

# 2. Replace binary
cp /path/to/new/pg-warehouse ./pg-warehouse

# 3. Run init (idempotent — creates any new schemas without touching existing data)
./pg-warehouse init --config pg-warehouse.yml

# 4. Clear stale lock (the old PID is gone)
sqlite3 .pgwh/state.db 'DELETE FROM lock_state;'

# 5. Restart CDC — reads confirmed_lsn from state, resumes where it left off
nohup ./pg-warehouse cdc start --config pg-warehouse.yml > cdc.log 2>&1 &

# 6. Validate
pg-warehouse doctor --config pg-warehouse.yml
tail -5 cdc.log
```

### When to use `--from-lsn`

| Scenario | Use `--from-lsn`? |
|---|:---:|
| First-time setup (pre-seeded DuckDB with bulk copy) | **Yes** (once) |
| Binary upgrade / redeploy | **No** |
| Crash recovery / restart | **No** |
| Full reset (teardown + setup + re-seed) | **Yes** |

---

## Frequently Asked Question

### How to Build a pg-warehouse binary ?

```bash
go build -o pg-warehouse ./cmd/pg-warehouse/
```

### How to build a pg-warehouse configuration file ?

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

### How to Initialize pg-warehouse ?

```bash
pg-warehouse init --duckdb ./warehouse.duckdb
```

Creates `warehouse.duckdb` (with `raw`, `stage`, `silver`, `feat` schemas) and `.pgwh/state.db` (SQLite state).

### How to Validate pg-warehouse ?

```bash
pg-warehouse doctor
```

### How to Sync Data into pg-warehouse ?

```bash
pg-warehouse sync
```

First run does a full snapshot. Subsequent runs use watermark-based incremental sync.

### How to start CDC (Real-Time Streaming) in pg-warehouse ?

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

### How to Inspect pg-warehouse ?

```bash
pg-warehouse inspect tables             # List all warehouse tables
pg-warehouse inspect schema raw.orders   # Describe a table
pg-warehouse inspect sync-state          # Show sync state
```

### How to Run Feature SQL in pg-warehouse ?

```bash
# Silver layer: curated intermediate transforms
pg-warehouse run \
  --sql-file ./sql/silver/order_enriched.sql \
  --target-table silver.order_enriched

# Gold layer: analytics-ready features with export
pg-warehouse run \
  --sql-file ./sql/feat/customer_features.sql \
  --target-table feat.customer_features \
  --output ./out/customer_features.parquet \
  --file-type parquet
```

> For a complete walkthrough of building SQL pipelines (silver and feat layers), naming conventions, and the CDC pause/resume pattern, see [Development Workflow](08-development-workflow.md).

### How to Preview and Export from pg-warehouse ?

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
# Single embedded database file containing four schemas (Medallion Architecture):
#   raw.*    — Bronze: mirrored source tables (written by sync and CDC)
#   stage.*  — Internal: temporary merge buffer for incremental sync (auto-managed)
#   silver.* — Silver: user curated/transformed intermediate tables
#   feat.*   — Gold: analytics-ready feature pipeline outputs (written by 'run' command)
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
| `duckdb` | Path to the DuckDB warehouse file (single file, four schemas) | Yes |
| `state` | Path to the SQLite state DB that tracks sync/CDC progress | No (defaults to `.pgwh/state.db`) |
| `cdc` | CDC configuration: publication name, slot, and table list | No (disabled by default) |
| `sync` | Batch sync mode, batch size, and per-table mapping | Yes (if using sync) |
| `run` | Default output directory and file format for feature pipelines | No (has defaults) |
| `logging` | Log level and format | No (defaults to `info` / `text`) |

## Pre-Seeding Details

### Consistency Notes

- **`COPY TO` in a `REPEATABLE READ` transaction** guarantees all tables are exported at exactly the same point in time. This is the same snapshot isolation mechanism PostgreSQL uses internally for `pg_dump` backups. It is production-safe — read-only, respects connection limits, and does not block writes.
- **The replication slot** created during `cdc setup` guarantees PostgreSQL retains all WAL from the moment it was created. No changes are lost between the LSN capture and the start of CDC streaming.
- **The WAL delta** between the captured LSN and when CDC starts is typically small (seconds to minutes of changes). CDC catches up this delta automatically before transitioning to live streaming.

### Why not pg_dump?

`pg_dump --format=directory` is the standard tool for PostgreSQL-to-PostgreSQL migrations, but it produces a custom binary format (`.dat` files) that only `pg_restore` can read — and `pg_restore` can only load into PostgreSQL, not DuckDB. Using `COPY TO CSV` instead produces standard CSV files that DuckDB's `read_csv` loads natively with automatic type detection.

| Method | Output Format | Loadable into DuckDB? | Production Safe |
|--------|--------------|:---------------------:|:---------------:|
| `COPY TO CSV` in `REPEATABLE READ` | Standard CSV | **Yes** (`read_csv`) | **Yes** |
| `pg_dump --format=directory` | PostgreSQL binary `.dat` | No (needs `pg_restore`) | Yes |
| `pg_dump --format=plain` | SQL INSERT statements | Technically (very slow) | Yes |
| DuckDB `postgres_scan` | Direct read | **Yes** (native) | **No** (high load) |

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

> **Do not use `postgres_scan` against production databases.** It performs full table scans without throttling or snapshot isolation. Use `COPY TO CSV` instead.

## pg-warehouse DuckDB Warehouse Architecture

> **pg-warehouse** uses a **single DuckDB database file** with four schemas following **Medallion Architecture for lakehouses**

| Schema | Layer | Purpose | Written By |
|--------|-------|---------|------------|
| `raw.*` | Bronze | Mirrored source tables — exact copies of PostgreSQL data | `sync`, `cdc start` |
| `stage.*` | Internal | Temporary merge buffer for incremental sync — auto-created and dropped per sync cycle (not user-facing) | `sync` (incremental mode) |
| `silver.*` | Silver | User curated/transformed intermediate tables — cleaned, joined, enriched data | `run --target-table silver.*` |
| `feat.*` | Gold | Analytics-ready feature pipeline outputs — aggregated, dashboard-ready tables | `run --target-table feat.*` |

```
PostgreSQL (source)
   ↓ CDC / sync
DuckDB (local warehouse)
   ├── raw.*    — Bronze: mirrored source tables
   ├── stage.*  — Internal: merge buffer (ephemeral, not user-facing)
   ├── silver.* — Silver: curated transforms (user SQL pipelines)
   └── feat.*   — Gold: analytics-ready outputs (user SQL pipelines)
   ↓
Parquet / CSV exports
```

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
