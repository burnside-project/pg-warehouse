-- Reference SQL: DuckDB schema definitions from SPEC.md.
-- NOT used by the application at runtime.
-- Actual DuckDB bootstrap is in internal/adapters/duckdb/bootstrap.go (raw, stage, feat schemas only).
-- All state/metadata is stored in SQLite — see internal/adapters/sqlitestate/schema.go.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS feat;
