# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `pg-warehouse demo` command for zero-dependency demo experience
- Rich terminal output with colors, tables, and progress indicators
- `--json` flag for machine-readable output on all commands
- Architecture Decision Records (ADRs)

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

[Unreleased]: https://github.com/burnside-project/pg-warehouse/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/burnside-project/pg-warehouse/releases/tag/v0.1.0
