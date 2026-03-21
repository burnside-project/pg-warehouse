# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0-rc.1] - 2026-03-21

### Added
- Multi-DuckDB architecture: `raw.duckdb` (CDC black box), `silver.duckdb` (development platform), `feature.duckdb` (analytics output)
- Versioned silver development: create, compare, promote, and rollback silver transforms (`v1`, `v2`, `current`)
- Generic SQL pipelines: SQL files are schema-free — pg-warehouse wires source (`v0`) and target (`v1`) transparently
- `--sql-dir` flag: discover and run all SQL files in a directory, sorted by numeric prefix
- `--target-schema` flag: override target schema for `--sql-dir` (default: derived from directory path)
- `--pipeline` flag: run all SQL in `sql/silver/v1/` and `sql/feat/` in one command
- `--refresh` flag: snapshot `raw.duckdb` into `silver.duckdb` v0 without stopping CDC
- `--promote` flag: swap `current.*` views to specified version
- Access guards with DANGER messages for reserved schemas (`raw`, `stage`, `v0`, `_meta`)
- CDC fast pre-seeding via `COPY TO CSV` + `--from-lsn` (minutes vs hours for 50M rows)
- CDC lag guardrails: `--drop-slot-on-exit`, `max_lag_bytes`, `health_check_sec`
- CDC auto-reconnect with exponential backoff (10 retries)
- Epoch-based CDC for consistency guarantees
- E-commerce recipe: 14 source tables → 5 silver → 5 feature → interactive dashboard + AI Q&A
- Data model semantic layer (YAML entities + metrics) for AI-grounded natural language queries
- `silver` subcommand: `create-version`, `list-versions`, `compare`, `drop-version`
- `epoch` subcommand: `list`, `status`
- systemd service documentation and deployment guide
- Windows amd64 binary (cross-compiled via mingw)
- `pg-warehouse demo` command for zero-dependency demo experience
- Rich terminal output with colors, tables, and progress indicators
- `--json` flag for machine-readable output on all commands

### Changed
- Configuration uses `duckdb.raw` / `duckdb.silver` / `duckdb.feature` instead of `duckdb.path` (single-file mode still supported for backwards compatibility)
- SQL files are now generic — no schema prefixes needed (schema wiring handled by pg-warehouse)
- Silver SQL files moved to `sql/silver/v1/` subdirectory
- Pipeline no longer requires stopping CDC (multi-DuckDB file isolation)
- `run-pipeline.sh` rewritten to use `pg-warehouse run --refresh --pipeline`

### Removed
- Root `sql/` directory (canonical SQL lives in `examples/ecommerce-recipe/sql/`)
- `recipes/` directory (internal-only, replaced by `examples/`)

## [0.1.0] - 2025-06-01

### Added
- Snapshot sync: full table mirror from PostgreSQL to DuckDB
- Incremental sync: watermark-based delta updates
- CDC streaming: PostgreSQL logical replication via pglogrepl
- DuckDB local warehouse with schema organization (raw/stage/feat/meta)
- SQL feature pipelines: write SQL files targeting `feat.*` schema
- Parquet and CSV export with file-level control
- SQLite state management: sync watermarks, CDC LSN, run history, audit log
- `init` command: project scaffolding and DuckDB/state DB creation
- `sync` command: full and incremental sync orchestration
- `cdc` command: setup, start, status, and teardown of logical replication
- `run` command: execute SQL feature files with export
- `preview` command: preview SQL query results
- `export` command: export warehouse tables to Parquet/CSV
- `inspect` command: tables, schema, and sync-state introspection
- `doctor` command: configuration and connectivity validation
- YAML configuration with validation
- Docker and Docker Compose support
- Homebrew tap distribution
- Cross-platform builds (Linux/macOS, amd64/arm64)

[Unreleased]: https://github.com/burnside-project/pg-warehouse/compare/v1.0.0-rc.1...HEAD
[1.0.0-rc.1]: https://github.com/burnside-project/pg-warehouse/compare/v0.1.0...v1.0.0-rc.1
[0.1.0]: https://github.com/burnside-project/pg-warehouse/releases/tag/v0.1.0
