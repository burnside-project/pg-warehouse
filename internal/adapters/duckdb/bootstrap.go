package duckdb

import "context"

// bootstrapSQL contains the DDL to initialize the DuckDB warehouse schemas.
// State and metadata are stored in SQLite (see sqlitestate adapter), not DuckDB.
const bootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS feat;
`

// silverBootstrapSQL initialises the silver development platform database.
const silverBootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS v0;
CREATE SCHEMA IF NOT EXISTS current;
CREATE SCHEMA IF NOT EXISTS _meta;
CREATE TABLE IF NOT EXISTS _meta.versions (
    version     INTEGER PRIMARY KEY,
    label       TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'active',
    epoch_id    INTEGER,
    description TEXT,
    created_at  TIMESTAMP DEFAULT current_timestamp,
    promoted_at TIMESTAMP
);
CREATE SEQUENCE IF NOT EXISTS _meta.refresh_log_seq;
CREATE TABLE IF NOT EXISTS _meta.refresh_log (
    id           INTEGER PRIMARY KEY DEFAULT nextval('_meta.refresh_log_seq'),
    refreshed_at TIMESTAMP DEFAULT current_timestamp,
    source       TEXT,
    epoch_id     INTEGER,
    tables       INTEGER,
    total_rows   BIGINT,
    duration_ms  INTEGER
);
`

// featureBootstrapSQL initialises the feature analytics output database.
const featureBootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS v0;
CREATE SCHEMA IF NOT EXISTS current;
CREATE SCHEMA IF NOT EXISTS _meta;
CREATE TABLE IF NOT EXISTS _meta.versions (
    version     INTEGER PRIMARY KEY,
    label       TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'active',
    epoch_id    INTEGER,
    description TEXT,
    created_at  TIMESTAMP DEFAULT current_timestamp,
    promoted_at TIMESTAMP
);
CREATE SEQUENCE IF NOT EXISTS _meta.refresh_log_seq;
CREATE TABLE IF NOT EXISTS _meta.refresh_log (
    id           INTEGER PRIMARY KEY DEFAULT nextval('_meta.refresh_log_seq'),
    refreshed_at TIMESTAMP DEFAULT current_timestamp,
    source       TEXT,
    epoch_id     INTEGER,
    tables       INTEGER,
    total_rows   BIGINT,
    duration_ms  INTEGER
);
`

// BootstrapSilver creates schemas and metadata tables for the silver database.
func (w *Warehouse) BootstrapSilver(ctx context.Context) error {
	return w.ExecuteSQL(ctx, silverBootstrapSQL)
}

// BootstrapFeature creates schemas for the feature database.
func (w *Warehouse) BootstrapFeature(ctx context.Context) error {
	return w.ExecuteSQL(ctx, featureBootstrapSQL)
}
