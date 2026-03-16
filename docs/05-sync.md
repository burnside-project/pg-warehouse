# Data Synchronization

pg-warehouse supports three sync modes for mirroring PostgreSQL data into DuckDB:

## Sync Modes

### 1. Full Snapshot

Copies the entire source table into `raw.*`, replacing existing data.

```
PostgreSQL (SELECT * FROM table)
   ↓
DuckDB (DROP + CREATE TABLE raw.table)
```

**When used:**
- First sync of a table (no previous watermark)
- No watermark column configured
- Explicit full sync requested

### 2. Watermark-Based Incremental

Fetches only rows where the watermark column exceeds the last known value.

```
PostgreSQL (SELECT * FROM table WHERE updated_at > '2024-01-15T10:30:00Z')
   ↓
DuckDB stage.table (temporary)
   ↓ merge by primary key
DuckDB raw.table (updated)
```

**When used:**
- Watermark column configured AND previous watermark exists in state DB
- Ideal for tables with `updated_at` or monotonically increasing columns

**Configuration:**
```yaml
sync:
  tables:
    - name: public.orders
      primary_key: [id]
      watermark_column: updated_at
```

### 3. CDC (Logical Replication)

Continuous streaming of INSERT/UPDATE/DELETE events from PostgreSQL WAL.

See [CDC documentation](04-cdc.md) for details.

## Sync Flow

```
1. Load sync state from SQLite (.pgwh/state.db)
2. For each configured table:
   a. Get last watermark from state
   b. Determine mode (full if no watermark, incremental otherwise)
   c. Fetch column schema from PostgreSQL (for type-aware table creation)
   d. Fetch rows (full or incremental)
   e. Write to DuckDB:
      - Full: DROP + CREATE raw.table with proper column types
      - Incremental: CREATE stage.table → merge into raw.table by primary keys
   f. Update sync state in SQLite (watermark, timestamp, row count, status)
   g. Record sync history
3. Report results
```

## Type Mapping

When creating tables in DuckDB, PostgreSQL types are mapped automatically:

| PostgreSQL | DuckDB |
|-----------|--------|
| integer, int4 | INTEGER |
| bigint, int8 | BIGINT |
| smallint, int2 | SMALLINT |
| boolean, bool | BOOLEAN |
| numeric, decimal | DOUBLE |
| real, float4 | FLOAT |
| double precision | DOUBLE |
| text, varchar | VARCHAR |
| timestamp | TIMESTAMP |
| timestamptz | TIMESTAMPTZ |
| date | DATE |
| time | TIME |
| uuid | UUID |
| json, jsonb | JSON |
| bytea | BLOB |

## Batch Processing

- Rows are inserted in batches of 1,000 for performance
- Multi-row `INSERT INTO ... VALUES (...), (...), (...)` per batch
- Incremental sync uses DELETE + INSERT merge pattern by primary keys

## CLI Usage

```bash
# Sync all configured tables
pg-warehouse sync

# Output shows per-table results
  OK    public.orders: mode=full rows=15000 duration=1.2s
  OK    public.customers: mode=incremental rows=42 duration=0.3s
```

## State Tracking

After each sync, the following is persisted to SQLite:

- `sync_state.last_watermark` — highest watermark value seen
- `sync_state.last_sync_at` — timestamp of sync completion
- `sync_state.last_status` — "success" or "failed"
- `sync_state.row_count` — number of rows synced
- `sync_history` — bounded history of all sync runs (max 500 entries)
