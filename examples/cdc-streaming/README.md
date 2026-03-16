# CDC Streaming

Stream PostgreSQL changes in real-time to DuckDB using logical replication.

## Prerequisites

PostgreSQL must have `wal_level=logical` configured. Your user needs `REPLICATION` privilege.

## Setup

1. Add CDC config to your `pg-warehouse.yml`:

```yaml
cdc:
  enabled: true
  publication_name: pgwh_pub
  slot_name: pgwh_slot
  tables:
    - public.orders
    - public.customers
```

2. Create the publication and replication slot:

```bash
pg-warehouse cdc setup --config pg-warehouse.yml
```

3. Start streaming (runs in foreground, Ctrl+C to stop):

```bash
pg-warehouse cdc start --config pg-warehouse.yml
```

4. Check status:

```bash
pg-warehouse cdc status --config pg-warehouse.yml
```

Changes in PostgreSQL will appear in DuckDB `raw.*` tables in near real-time.
