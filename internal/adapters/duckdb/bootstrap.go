package duckdb

// bootstrapSQL contains the DDL to initialize the DuckDB warehouse schemas.
// State and metadata are stored in SQLite (see sqlitestate adapter), not DuckDB.
const bootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS feat;
`
