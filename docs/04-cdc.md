# Change Data Capture (CDC)

pg-warehouse supports production-grade PostgreSQL CDC using **logical replication** with the `pgoutput` protocol.

## How It Works

```
PostgreSQL (wal_level=logical)
   ↓ CREATE PUBLICATION + REPLICATION SLOT
   ↓
pg-warehouse cdc start
   ↓ Initial snapshot (consistent point-in-time copy)
   ↓ WAL streaming (INSERT/UPDATE/DELETE events)
   ↓ Batched apply to DuckDB raw.* tables
   ↓ LSN confirmation every 10 seconds
   ↓
DuckDB warehouse (raw.* tables kept in sync)
```

## Prerequisites

### PostgreSQL Configuration

```sql
-- Check current WAL level
SHOW wal_level;  -- Must be 'logical'

-- If not logical, update postgresql.conf:
-- wal_level = logical
-- max_replication_slots = 4  (at least 1 for pg-warehouse)
-- max_wal_senders = 4        (at least 1 for pg-warehouse)
```

Restart PostgreSQL after changing `wal_level`.

### User Permissions

The PostgreSQL user needs `REPLICATION` privilege:

```sql
ALTER USER myuser REPLICATION;
```

Or use a superuser for initial setup.

## Configuration

```yaml
postgres:
  url: postgres://user:pass@localhost:5432/mydb
  schema: public

cdc:
  enabled: true
  publication_name: pgwh_pub
  slot_name: pgwh_slot
  tables:
    - public.orders
    - public.customers

sync:
  tables:
    - name: public.orders
      primary_key: [id]
    - name: public.customers
      primary_key: [id]
```

Primary keys in `sync.tables` are required for CDC UPDATE and DELETE operations.

## Commands

### Setup

Creates the PostgreSQL publication and replication slot:

```bash
pg-warehouse cdc setup
```

This executes:
```sql
CREATE PUBLICATION pgwh_pub FOR TABLE public.orders, public.customers;
-- Creates replication slot 'pgwh_slot' with pgoutput plugin
```

### Start Streaming

```bash
pg-warehouse cdc start
```

Lifecycle:
1. **Acquire lock** — prevents concurrent CDC processes
2. **Initial snapshot** — for tables without a confirmed LSN, performs a full consistent copy
3. **WAL streaming** — receives INSERT/UPDATE/DELETE events via logical replication
4. **Batch apply** — flushes events to DuckDB every 100 events or 1 second
5. **LSN confirmation** — confirms progress to PostgreSQL every 10 seconds
6. **Graceful shutdown** — Ctrl+C confirms final LSN, saves state, releases lock

### Check Status

```bash
pg-warehouse cdc status
```

Output:
```
Replication Slot: pgwh_slot
  Plugin:        pgoutput
  Active:        true
  Confirmed LSN: 0/16B3748
  Current LSN:   0/16B3790
  Lag:           72 bytes

Table States:
  public.orders:    status=streaming confirmed_lsn=0/16B3748
  public.customers: status=streaming confirmed_lsn=0/16B3748
```

### Teardown

```bash
pg-warehouse cdc teardown
```

Drops the publication and replication slot from PostgreSQL.

## Event Processing

### INSERT
New row inserted into `raw.*` table via `InsertRows`.

### UPDATE
1. DELETE existing row by primary key from `raw.*`
2. INSERT new row values

### DELETE
DELETE row by primary key from `raw.*`.

## State Persistence

CDC state is stored in SQLite (`.pgwh/state.db`), not DuckDB:

- **Per-table state** — `cdc_state` table tracks confirmed LSN per source table
- **Watermarks** — `cdc_confirmed_lsn` watermark for global progress
- **Audit trail** — all CDC events logged (setup, start, stop, errors)

### Recovery

If pg-warehouse crashes or is killed:

1. Restart `pg-warehouse cdc start`
2. Lock is re-acquired (expired lock from crashed process is reclaimed)
3. Streaming resumes from **last confirmed LSN** in SQLite state
4. PostgreSQL replays events from that LSN (no data loss)
5. Events already applied to DuckDB are idempotent (DELETE + INSERT pattern)

## Protocol Details

- **Plugin:** `pgoutput` (built into PostgreSQL 10+)
- **Protocol version:** 2 (supports streaming transactions)
- **Message types handled:** RelationMessage, InsertMessage, UpdateMessage, DeleteMessage, BeginMessage, CommitMessage, TruncateMessage
- **Library:** `github.com/jackc/pglogrepl`
- **Connection:** Dedicated replication connection via `pgconn` with `replication=database` parameter

## Limitations

- Requires `wal_level=logical` (PostgreSQL restart needed if changing)
- One replication slot per pg-warehouse instance
- DDL changes (ALTER TABLE) require re-setup
- TRUNCATE events are received but not yet applied
- Large initial snapshots load all rows into memory
