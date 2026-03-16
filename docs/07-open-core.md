# Open-Core Strategy

pg-warehouse follows an open-core model: the developer workflow is open source, production operations are commercial.

## Product Rule

> If it helps an individual developer work locally, it belongs in OSS.
> If it helps an organization operate in production, it belongs in paid.

## OSS (Community Edition)

Everything a developer needs for the local workflow:

- Local CLI
- YAML configuration
- PostgreSQL connectivity (pgx/v5 with connection pooling)
- Full snapshot sync
- Watermark-based incremental sync
- **PostgreSQL CDC via logical replication** (pglogrepl)
- Local DuckDB warehouse
- Table/schema inspection
- SQL feature file execution
- Parquet/CSV export
- SQLite state database (audit trail, watermarks, locking)
- Unit and integration tests

## Planned Commercial Features

Production operations, governance, and platform management:

| Category | Features |
|----------|----------|
| **Sync** | Low-latency streaming sync, parallel table sync |
| **Scheduling** | Recurring sync jobs, daemon/background mode |
| **Cloud Export** | S3, GCS, Iceberg export adapters |
| **State** | Remote metadata store, multi-instance coordination |
| **Observability** | Cloud dashboard, metrics, alerting, notifications |
| **Governance** | RBAC, audit logging (cloud), data quality checks, drift detection |
| **Lineage** | Lineage UI, feature registry |
| **Operations** | Agent fleet management, enterprise support |

## Extension Points

The hexagonal architecture provides clean extension seams:

```
Current (OSS):
  PostgresSource → postgres.Source (pgx pool)
  CDCSource      → postgres.CDCAdapter (pglogrepl)
  WarehouseStore → duckdb.Warehouse
  StateStore     → sqlitestate.Store
  Exporter       → parquet.Exporter, csv.Exporter

Future (Paid):
  WarehouseStore → iceberg.Store
  StateStore     → remote.Store (cloud-backed)
  Exporter       → s3.Exporter, gcs.Exporter
  Scheduler      → cron.Scheduler
  ControlPlane   → cloud.Agent
```

New adapters implement existing port interfaces — no service changes needed.
