# Docker Compose Local Demo

Run pg-warehouse with a local PostgreSQL database in 30 seconds.

## Quick Start

From the repo root:

```bash
docker compose up --build
```

This starts:
- PostgreSQL 16 with `wal_level=logical` and sample data (10 customers, 100 orders)
- pg-warehouse initialized with DuckDB warehouse

## Manual Commands

After the containers are running, exec into the pg-warehouse container:

```bash
# Sync data from PostgreSQL to DuckDB
docker compose exec pg-warehouse pg-warehouse sync --config pg-warehouse.yml

# Inspect synced tables
docker compose exec pg-warehouse pg-warehouse inspect tables --config pg-warehouse.yml

# Run a feature query
docker compose exec pg-warehouse pg-warehouse run \
  --config pg-warehouse.yml \
  --sql-file /app/sql/customer_features.sql \
  --target-table feat.customer_features \
  --output /app/data/out/features.parquet \
  --file-type parquet

# Export to CSV
docker compose exec pg-warehouse pg-warehouse export \
  --config pg-warehouse.yml \
  --table raw.orders \
  --output /app/data/out/orders.csv \
  --file-type csv
```
