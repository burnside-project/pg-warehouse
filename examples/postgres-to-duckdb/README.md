# Postgres to DuckDB

Mirror PostgreSQL tables into a local DuckDB warehouse.

## Setup

1. Create a config file pointing to your PostgreSQL:

```yaml
postgres:
  url: postgres://user:pass@localhost:5432/mydb
duckdb:
  raw: ./raw.duckdb
sync:
  tables:
    - name: public.orders
      primary_key: [id]
      watermark_column: updated_at
```

2. Initialize and sync:

```bash
pg-warehouse init --config pg-warehouse.yml
pg-warehouse sync --config pg-warehouse.yml
pg-warehouse inspect tables --config pg-warehouse.yml
```
