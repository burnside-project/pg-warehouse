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

## Why pg-warehouse?

Getting data out of PostgreSQL for analytics or ML usually means stitching together Python scripts, cron jobs, and a cloud warehouse you don't need. pg-warehouse replaces that with a single binary: sync tables into an embedded DuckDB, run SQL feature pipelines, and export to Parquet or CSV. Everything runs locally, on your machine, with no external dependencies.

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

## Install

```console
$ brew install burnside-project/tap/pg-warehouse
$ go install github.com/burnside-project/pg-warehouse/cmd/pg-warehouse@latest
$ docker run --rm ghcr.io/burnside-project/pg-warehouse sync
```

## Quickstart (2 minutes)

**1. Initialize** -- creates config, DuckDB warehouse, and state DB:

```console
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

**4. Run a SQL pipeline** -- transform data and export:

```console
$ pg-warehouse run \
    --sql-file ./sql/customer_features.sql \
    --target-table feat.customer_features \
    --output ./out/customer_features.parquet \
    --file-type parquet
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
