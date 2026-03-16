# ADR 0002: DuckDB as Local Warehouse

**Date:** 2025-06-01
**Status:** Accepted

## Context

pg-warehouse needs an embedded analytical store that can handle columnar queries over mirrored PostgreSQL data without requiring users to install or manage external services. The warehouse must support standard SQL and perform well on analytical workloads (aggregations, joins over wide tables, feature engineering queries).

## Alternatives Considered

- **SQLite** — Excellent embedded database, but row-oriented storage is poorly suited for analytical query patterns.
- **ClickHouse** — Strong analytical engine, but requires a running server process. Conflicts with the local-first, zero-dependency goal.
- **Polars** — Fast DataFrame library, but not SQL-first and would require a custom query layer.

## Decision

Use DuckDB as the embedded local warehouse via `marcboeker/go-duckdb`. DuckDB provides columnar storage, vectorized execution, native Parquet/CSV export, and a full SQL dialect — all in-process with no external dependencies.

Data is organized into DuckDB schemas: `raw.*` for mirrored source tables, `stage.*` for incremental merge staging, and `feat.*` for SQL feature pipeline outputs.

## Consequences

- **Positive:** Analytical queries run at columnar speed with no infrastructure to manage.
- **Positive:** Native Parquet export eliminates the need for a separate serialization layer.
- **Positive:** Standard SQL interface means users do not need to learn a new query language.
- **Negative:** CGO dependency (via go-duckdb) complicates cross-compilation and CI setup.
- **Negative:** DuckDB files are single-writer; concurrent access requires coordination.
