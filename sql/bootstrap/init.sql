CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS feat;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.sync_state (
    table_name TEXT PRIMARY KEY,
    sync_mode TEXT,
    watermark_column TEXT,
    last_watermark TEXT,
    last_lsn TEXT,
    last_snapshot_at TIMESTAMP,
    last_sync_at TIMESTAMP,
    last_status TEXT,
    row_count BIGINT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS meta.sync_history (
    run_id TEXT,
    table_name TEXT,
    sync_mode TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    inserted_rows BIGINT,
    updated_rows BIGINT,
    deleted_rows BIGINT,
    status TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS meta.feature_runs (
    run_id TEXT,
    sql_file TEXT,
    target_table TEXT,
    output_path TEXT,
    output_type TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    row_count BIGINT,
    status TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS meta.feature_dependencies (
    run_id TEXT,
    source_table TEXT,
    target_table TEXT
);
