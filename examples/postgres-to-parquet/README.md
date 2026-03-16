# Postgres to Parquet

Sync PostgreSQL data and export as Parquet files.

## Setup

1. Sync your data:

```bash
pg-warehouse sync --config pg-warehouse.yml
```

2. Write a feature SQL file (`features.sql`):

```sql
CREATE OR REPLACE TABLE feat.order_summary AS
SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM raw.orders
GROUP BY customer_id;
```

3. Run and export:

```bash
pg-warehouse run \
  --config pg-warehouse.yml \
  --sql-file features.sql \
  --target-table feat.order_summary \
  --output ./out/order_summary.parquet \
  --file-type parquet
```
