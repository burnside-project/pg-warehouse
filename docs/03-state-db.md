# SQLite State Database

pg-warehouse uses an independent SQLite database (`.pgwh/state.db`) to track all platform state. This design ensures state survives DuckDB warehouse rebuilds, prevents concurrent execution conflicts, and provides a bounded audit trail.

## Location

Default: `.pgwh/state.db` (relative to project root)

Configurable via:
```yaml
state:
  path: .pgwh/state.db
```

## Design Principles

Adopted from the [project-collector-agent](https://github.com/dataalgebra-engineering/project-collector-agent) battle-tested patterns:

1. **Single-writer serialization** — `MaxOpenConns=1` prevents write conflicts
2. **WAL mode** — Write-Ahead Logging for concurrent reads during writes
3. **Bounded tables** — Automatic cleanup triggers prevent unbounded growth
4. **Singleton tables** — `CHECK (id = 1)` enforces single-row tables
5. **Pure Go driver** — `modernc.org/sqlite` (no CGO dependency)

## Connection Configuration

```
journal_mode  = WAL
synchronous   = NORMAL
busy_timeout  = 5000ms
max_open_conns = 1
```

## Schema

### project_identity (singleton)

Tracks which PostgreSQL source this project connects to.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Always 1 |
| project_name | TEXT | Project name from config |
| pg_url_hash | TEXT | SHA-256 of PG connection URL |
| warehouse_path | TEXT | Path to DuckDB file |
| created_at | TEXT | ISO 8601 timestamp |

### sync_state (per-table)

Current sync watermark and status for each table.

| Column | Type | Description |
|--------|------|-------------|
| table_name | TEXT PK | Source table name |
| sync_mode | TEXT | "full" or "incremental" |
| watermark_column | TEXT | Column used for incremental sync |
| last_watermark | TEXT | Last seen watermark value |
| last_lsn | TEXT | PostgreSQL LSN (for CDC) |
| last_snapshot_at | TEXT | Last full snapshot timestamp |
| last_sync_at | TEXT | Last sync timestamp |
| last_status | TEXT | "success" or "failed" |
| row_count | INTEGER | Rows synced in last run |
| error_message | TEXT | Error details if failed |

### sync_history (bounded, max 500)

Append-only sync run history. Automatically cleaned up via trigger (deletes oldest 50 when exceeding 500 entries).

### feature_runs (bounded, max 200)

Feature job execution records. Cleanup trigger at 200 entries.

### cdc_state (per-table)

CDC replication state tracking.

| Column | Type | Description |
|--------|------|-------------|
| table_name | TEXT PK | Source table name |
| slot_name | TEXT | Replication slot name |
| publication_name | TEXT | Publication name |
| confirmed_lsn | TEXT | Last confirmed LSN position |
| last_received_lsn | TEXT | Last received LSN |
| status | TEXT | streaming, snapshot, stopped, error |
| error_message | TEXT | Error details |
| updated_at | TEXT | Last update timestamp |

### audit_log (bounded, max 1000)

Platform audit trail. Cleanup trigger deletes oldest 100 entries when exceeding 1000.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| timestamp | TEXT | ISO 8601 timestamp |
| level | TEXT | info, warn, error, critical |
| event | TEXT | Event type (e.g., sync.start, cdc.setup) |
| message | TEXT | Human-readable message |
| metadata | TEXT | JSON metadata |

### watermarks (named checkpoints)

Named progress checkpoints for exactly-once semantics.

### lock_state (singleton)

Prevents concurrent `pg-warehouse` execution.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Always 1 |
| holder_pid | INTEGER | Process ID holding the lock |
| holder_host | TEXT | Hostname of lock holder |
| acquired_at | TEXT | Lock acquisition time |
| expires_at | TEXT | Lock expiry time (TTL-based) |

### schema_version (singleton)

Tracks schema version for migrations. Current version: 1.

## Bounded Cleanup Triggers

```sql
-- Audit: keep 1000, delete 100 oldest
CREATE TRIGGER audit_cleanup AFTER INSERT ON audit_log
WHEN (SELECT COUNT(*) FROM audit_log) > 1000
BEGIN
    DELETE FROM audit_log WHERE id IN (
        SELECT id FROM audit_log ORDER BY id ASC LIMIT 100
    );
END;
```

Same pattern applied to `sync_history` (500/50) and `feature_runs` (200/20).

## State Durability

The key architectural benefit: **state survives DuckDB rebuilds**.

```
Scenario: DuckDB file corrupted
  1. Delete warehouse.duckdb
  2. pg-warehouse init
  3. DuckDB rebuilt with empty raw/stage/feat schemas
  4. .pgwh/state.db still has all sync watermarks
  5. Next sync resumes from last watermark (no full re-sync needed)
```

## Concurrent Execution Safety

```
Process A: pg-warehouse cdc start
  → TryAcquireLock(pid=1234, host="laptop", ttl=24h) → true
  → Streaming...

Process B: pg-warehouse sync
  → TryAcquireLock(pid=5678, host="laptop", ttl=1h) → false
  → Error: "another pg-warehouse process is running (PID 1234)"
```

Expired locks are automatically reclaimed.
