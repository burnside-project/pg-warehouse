<!-- Logo placeholder -->
<p align="center">
  <strong>pg-warehouse</strong>
</p>

<p align="center">
  Mirror PostgreSQL &rarr; DuckDB. Run SQL pipelines. Export Parquet. No cloud required.
</p>

<p align="center">
  <a href="https://github.com/burnside-project/pg-warehouse/actions"><img src="https://img.shields.io/github/actions/workflow/status/burnside-project/pg-warehouse/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/burnside-project/pg-warehouse/releases"><img src="https://img.shields.io/github/v/release/burnside-project/pg-warehouse" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
  <a href="https://img.shields.io/github/go-mod/go-version/burnside-project/pg-warehouse"><img src="https://img.shields.io/github/go-mod/go-version/burnside-project/pg-warehouse" alt="Go"></a>
  <a href="https://github.com/burnside-project/pg-warehouse/stargazers"><img src="https://img.shields.io/github/stars/burnside-project/pg-warehouse?style=social" alt="Stars"></a>
</p>

---
![1.png](assets/1.png)
---
![2.png](assets/2.png)
---
![3.png](assets/3.png)


```console
$ pg-warehouse init --pg-url postgres://user:pass@localhost:5432/appdb
Initialized warehouse at ./warehouse.duckdb

$ pg-warehouse sync
Syncing 4 tables...
  orders         12,841 rows  (incremental, watermark: updated_at)
  customers       3,209 rows  (full snapshot)
  products          487 rows  (full snapshot)
  order_items    31,002 rows  (CDC)
Sync complete in 2.4s

$ pg-warehouse run --sql-file ./sql/customer_features.sql --output ./out/customer_features.parquet
Wrote 3,209 rows to ./out/customer_features.parquet (428 KB)
```

## Documentation

| Doc | Description |
|-----|-------------|
| [Architecture](docs/01-architecture.md) | Hexagonal design, layers, port interfaces |
| [Quickstart](docs/02-quickstart.md) | Full walkthrough with examples |
| [State Database](docs/03-state-db.md) | SQLite schema and semantics |
| [CDC Guide](docs/04-cdc.md) | Logical replication setup and lifecycle |
| [Sync Modes](docs/05-sync.md) | Full vs. incremental vs. CDC |
| [Configuration](docs/06-configuration.md) | YAML reference |
| [Open-Core Strategy](docs/07-open-core.md) | OSS vs. commercial boundary |
| [Development Workflow](docs/08-development-workflow.md) | SQL pipelines: raw → silver → feat |
| [Multi-DuckDB Architecture](docs/09-multi-duckdb-architecture.md) | Zero-downtime CDC with three DuckDB files |
| [Silver Versioning](docs/10-silver-versioning.md) | Versioned silver development: create, compare, promote |
| [Data Model and AI](docs/11-data-model-and-ai.md) | Semantic layer (YAML) + AI-powered Q&A |


## Why pg-warehouse?

Getting data out of PostgreSQL for analytics or ML usually means stitching together 
Python scripts, cron jobs, and a cloud warehouse you don't need. pg-warehouse replaces that with 
a single binary: sync tables into an embedded DuckDB, run SQL feature pipelines, 
and export to Parquet or CSV. Everything runs locally, on your machine, with no external dependencies.

## What makes pg-warehouse a local-first Data Warehouse?

| Features              | Exist ? |
|-----------------------|---------|
| Analytical Storage (Columnar + Optimized for Reads) | ✅       |
| Separation from OLTP (Workload Isolation) | ✅       |
| SQL Analytical Engine | ✅       |
| Data Transformation Layer (ETL / ELT) | ✅       |
| Durable Analytical Storage (Files or Tables) | ✅       |

## Quick comparison

| | pg-warehouse | pg_dump | Airbyte | dbt |
|---|---|---|---|---|
| PostgreSQL sync | Full, incremental, CDC | Full only | Full, incremental, CDC | -- |
| Local analytics | DuckDB (columnar) | -- | -- | DuckDB adapter |
| Parquet/CSV export | Built-in | -- | Via connectors | Via packages |
| SQL pipelines | Built-in | -- | -- | Core strength |
| Infrastructure | Single binary | Single binary | Docker + Java | Python + adapter |
| Cloud required | No | No | Optional | Optional |
| Scheduling | -- | cron | Built-in | Built-in |

## What does it solve?
A local-first Data Warehouse engine that mirrors PostgreSQL data into DuckDB using native PostgreSQL CDC.
Best for teams that want CDC + SQL transforms + Parquet without standing up Kafka, Spark, or a warehouse

## How do I do data wrangling?
Built-in Transformation pipeline using SQL and exports analytics datasets to Parquet.
https://github.com/burnside-project/pg-warehouse/blob/main/docs/08-development-workflow.md

## Is it Simple and Cost Effective?
Instead of building complex pipelines with Kafka, Spark, and cloud warehouses, pg-warehouse 
lets you run analytics pipelines locally on your PostgreSQL with just SQL!
https://github.com/burnside-project/pg-warehouse/blob/main/docs/01-architecture.md


## Do I need complex pipeline from PostgreSQL?
No. pg-warehouse uses production-grade PostgreSQL native CDC logical replication + LSN with the pgoutput protocol.
This is like running a replication node and we are just ingesting raw data in real time using PostgreSQL Write Ahead Logs.
https://github.com/burnside-project/pg-warehouse/blob/main/docs/04-cdc.md

## How does it different from PostgreSQL OLAP Plugins?
PostgreSQL OLAP plugins → run analytics inside Postgres
pg-warehouse → moves analytics outside Postgres into DuckDB. 
pg-warehouse can runs completely isolated in another node
https://github.com/burnside-project/pg-warehouse/blob/main/docs/01-architecture.md

## How does it work?
pg-warehouse uses three DuckDB files following Medallion Architecture:

| File | Layer | Purpose |
|------|-------|---------|
| `raw.duckdb` | Bronze | CDC black box. Deduped PostgreSQL mirror. CDC owns it exclusively. |
| `silver.duckdb` | Silver | Development platform. v0 = raw copy, v1 = your transforms, current = production. |
| `feature.duckdb` | Gold | Analytics output. v0 = silver copy, v1 = aggregations, current = dashboards. |

```bash
# CDC streams continuously (never stops)
pg-warehouse cdc start

# Developer: refresh raw data, run transforms, promote
pg-warehouse run --refresh --pipeline --promote
```

https://github.com/burnside-project/pg-warehouse/blob/main/docs/09-multi-duckdb-architecture.md


## Install

```console
$ brew install burnside-project/tap/pg-warehouse
$ go install github.com/burnside-project/pg-warehouse/cmd/pg-warehouse@latest
$ docker run --rm ghcr.io/burnside-project/pg-warehouse sync
```

Or build from source:

```bash
go build -o pg-warehouse ./cmd/pg-warehouse/
```

## Deployment Layout

pg-warehouse runs from a single working directory. All paths in `pg-warehouse.yml` are relative to this directory.

```
~/pg-warehouse/                  # Working directory
├── pg-warehouse                 # Binary
├── pg-warehouse.yml             # Configuration
├── raw.duckdb                   # CDC black box (deduped PostgreSQL mirror)
├── silver.duckdb                # Silver development platform (v0 + v1 + current)
├── feature.duckdb               # Feature analytics output (v0 + v1 + current)
├── .pgwh/
│   └── state.db                 # SQLite state (sync/CDC/epoch progress)
├── sql/
│   ├── silver/v1/               # Silver SQL transforms (001.sql, 002.sql...)
│   └── feat/                    # Feature SQL transforms (001.sql, 002.sql...)
├── out/                         # Parquet/CSV exports
└── cdc.log                      # CDC log (when running via nohup)
```

For systemd-managed deployments, the service file should point to this directory:

```ini
[Service]
WorkingDirectory=/home/<user>/pg-warehouse
ExecStart=/home/<user>/pg-warehouse/pg-warehouse cdc start --config pg-warehouse.yml
```

## Quickstart (2 minutes)

**1. Initialize** -- creates config, DuckDB warehouse, and state DB:

```console
$ mkdir -p ~/pg-warehouse && cd ~/pg-warehouse
$ pg-warehouse init --pg-url postgres://user:pass@localhost:5432/appdb --duckdb ./warehouse.duckdb
```

**2. Sync** -- mirror PostgreSQL tables into DuckDB:

```console
$ pg-warehouse sync
```

**3. Inspect** -- verify what landed:

```console
$ pg-warehouse inspect tables
```

**4. Run the pipeline** -- refresh raw data, build silver + feature transforms, export:

```console
$ pg-warehouse run --refresh --pipeline --promote
```

Or run individual transforms:

```console
$ pg-warehouse run --refresh
$ pg-warehouse run --sql-file ./sql/silver/v1/001_order_enriched.sql
$ pg-warehouse run --sql-file ./sql/feat/001_sales_summary.sql --output ./out/sales_summary.parquet
```

**5. Validate setup**:

```console
$ pg-warehouse doctor
```

## Features

**Sync**
- [x] Full table snapshots
- [x] Incremental sync via watermark columns
- [x] CDC streaming via PostgreSQL logical replication (pglogrepl)
- [x] Automatic sync mode detection per table

**Analytics**
- [x] Embedded DuckDB columnar warehouse (Medallion Architecture)
- [x] SQL pipelines targeting `silver.*` (curated) and `feat.*` (analytics-ready) schemas
- [x] Multi-DuckDB mode: zero-downtime CDC with epoch-consistent reads
- [x] Versioned silver development: create, compare, promote, rollback
- [x] Data model semantic layer (YAML) for AI-powered Q&A
- [x] Preview query results before export
- [x] Fast pre-seeding via `COPY TO CSV` + `--from-lsn` (minutes vs. hours)

**Export**
- [x] Parquet export
- [x] CSV export

**Developer Experience**
- [x] Single binary, zero external dependencies
- [x] `doctor` command for config and connectivity validation
- [x] SQLite state tracking that survives warehouse rebuilds
- [x] YAML configuration

## E-Commerce Recipe

A complete working example with 14 source tables, Medallion pipeline, data model, Docker dashboard, and AI Q&A:

```bash
# Run the pipeline
./examples/ecommerce-recipe/run-pipeline.sh

# Launch the dashboard (with AI Q&A)
cd examples/ecommerce-recipe/dashboard
ANTHROPIC_API_KEY=sk-ant-... docker compose up --build
# Open http://localhost:8050
```

See [examples/ecommerce-recipe/README.md](examples/ecommerce-recipe/README.md) for full details.

## Architecture

pg-warehouse uses hexagonal architecture with clean port/adapter separation. The CLI layer (Cobra) calls services that depend only on port interfaces. Adapters for PostgreSQL, DuckDB, SQLite, Parquet, and CSV implement those interfaces. New sources, warehouses, and exporters plug in without changing business logic.

See [docs/01-architecture.md](docs/01-architecture.md) for the full design.

## Open Core

The open-source edition covers the full developer workflow: sync, CDC, DuckDB, SQL pipelines, and local export. Production operations -- scheduling, cloud storage export (S3/GCS/Iceberg), remote state, RBAC, and lineage -- are commercial.

See [docs/07-open-core.md](docs/07-open-core.md) for the boundary details.

## Community

- [GitHub Issues](https://github.com/burnside-project/pg-warehouse/issues) -- Bugs and feature requests
- [GitHub Discussions](https://github.com/burnside-project/pg-warehouse/discussions) -- Questions and ideas
- [Contributing](CONTRIBUTING.md) -- Development setup and guidelines
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)
<!-- - [Discord](https://discord.gg/placeholder) -- Chat with the community -->

## License

[Apache License 2.0](LICENSE) -- Copyright 2025-2026 [Burnside Project](https://burnsideproject.ai)
