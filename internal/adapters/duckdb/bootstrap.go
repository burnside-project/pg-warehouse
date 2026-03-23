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
CREATE SCHEMA IF NOT EXISTS v1;
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
CREATE TABLE IF NOT EXISTS _meta.version_files (
    version      INTEGER,
    filename     TEXT,
    checksum     TEXT,
    target_table TEXT,
    built_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (version, filename)
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
CREATE TABLE IF NOT EXISTS _meta.contracts (
    contract_name TEXT NOT NULL,
    version       INTEGER NOT NULL,
    layer         TEXT NOT NULL,
    schema_json   TEXT,
    grain         TEXT,
    primary_key   TEXT,
    owner         TEXT,
    status        TEXT DEFAULT 'active',
    file_path     TEXT,
    checksum      TEXT,
    registered_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (contract_name, version)
);
CREATE TABLE IF NOT EXISTS _meta.models (
    model_name      TEXT PRIMARY KEY,
    file_path       TEXT NOT NULL,
    checksum        TEXT NOT NULL,
    materialization TEXT NOT NULL DEFAULT 'table',
    layer           TEXT,
    depends_on_json TEXT,
    sources_json    TEXT,
    contract_name   TEXT,
    tags_json       TEXT,
    registered_at   TIMESTAMP DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS _meta.releases (
    release_name      TEXT NOT NULL,
    version           TEXT NOT NULL,
    description       TEXT,
    models_json       TEXT,
    input_json        TEXT,
    git_commit        TEXT,
    manifest_checksum TEXT,
    registered_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (release_name, version)
);
CREATE SEQUENCE IF NOT EXISTS _meta.build_id_seq;
CREATE TABLE IF NOT EXISTS _meta.builds (
    build_id        INTEGER PRIMARY KEY DEFAULT nextval('_meta.build_id_seq'),
    release_name    TEXT NOT NULL,
    release_version TEXT NOT NULL,
    git_commit      TEXT,
    input_epoch     INTEGER,
    environment     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    model_count     INTEGER,
    row_count       BIGINT,
    started_at      TIMESTAMP DEFAULT current_timestamp,
    finished_at     TIMESTAMP,
    duration_ms     INTEGER,
    error_message   TEXT
);
CREATE TABLE IF NOT EXISTS _meta.promotions (
    release_name    TEXT NOT NULL,
    release_version TEXT NOT NULL,
    environment     TEXT NOT NULL,
    build_id        INTEGER,
    promoted_by     TEXT,
    promoted_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (release_name, release_version, environment)
);
`

// featureBootstrapSQL initialises the feature analytics output database.
const featureBootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS v0;
CREATE SCHEMA IF NOT EXISTS v1;
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
CREATE TABLE IF NOT EXISTS _meta.version_files (
    version      INTEGER,
    filename     TEXT,
    checksum     TEXT,
    target_table TEXT,
    built_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (version, filename)
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
CREATE TABLE IF NOT EXISTS _meta.contracts (
    contract_name TEXT NOT NULL,
    version       INTEGER NOT NULL,
    layer         TEXT NOT NULL,
    schema_json   TEXT,
    grain         TEXT,
    primary_key   TEXT,
    owner         TEXT,
    status        TEXT DEFAULT 'active',
    file_path     TEXT,
    checksum      TEXT,
    registered_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (contract_name, version)
);
CREATE TABLE IF NOT EXISTS _meta.models (
    model_name      TEXT PRIMARY KEY,
    file_path       TEXT NOT NULL,
    checksum        TEXT NOT NULL,
    materialization TEXT NOT NULL DEFAULT 'table',
    layer           TEXT,
    depends_on_json TEXT,
    sources_json    TEXT,
    contract_name   TEXT,
    tags_json       TEXT,
    registered_at   TIMESTAMP DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS _meta.releases (
    release_name      TEXT NOT NULL,
    version           TEXT NOT NULL,
    description       TEXT,
    models_json       TEXT,
    input_json        TEXT,
    git_commit        TEXT,
    manifest_checksum TEXT,
    registered_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (release_name, version)
);
CREATE SEQUENCE IF NOT EXISTS _meta.build_id_seq;
CREATE TABLE IF NOT EXISTS _meta.builds (
    build_id        INTEGER PRIMARY KEY DEFAULT nextval('_meta.build_id_seq'),
    release_name    TEXT NOT NULL,
    release_version TEXT NOT NULL,
    git_commit      TEXT,
    input_epoch     INTEGER,
    environment     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    model_count     INTEGER,
    row_count       BIGINT,
    started_at      TIMESTAMP DEFAULT current_timestamp,
    finished_at     TIMESTAMP,
    duration_ms     INTEGER,
    error_message   TEXT
);
CREATE TABLE IF NOT EXISTS _meta.promotions (
    release_name    TEXT NOT NULL,
    release_version TEXT NOT NULL,
    environment     TEXT NOT NULL,
    build_id        INTEGER,
    promoted_by     TEXT,
    promoted_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (release_name, release_version, environment)
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

// EnsureSchema creates a schema if it does not exist.
func (w *Warehouse) EnsureSchema(ctx context.Context, schema string) error {
	return w.ExecuteSQL(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema)
}
