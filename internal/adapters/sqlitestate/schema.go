package sqlitestate

// currentSchemaVersion is the current schema version.
const currentSchemaVersion = 2

// bootstrapSQL contains the DDL to initialize the SQLite state database.
const bootstrapSQL = `
-- Project identity (singleton)
CREATE TABLE IF NOT EXISTS project_identity (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    project_name     TEXT NOT NULL,
    pg_url_hash      TEXT NOT NULL,
    warehouse_path   TEXT NOT NULL,
    created_at       TEXT DEFAULT (datetime('now'))
);

-- Sync state per table
CREATE TABLE IF NOT EXISTS sync_state (
    table_name       TEXT PRIMARY KEY,
    sync_mode        TEXT,
    watermark_column TEXT,
    last_watermark   TEXT,
    last_lsn         TEXT,
    last_snapshot_at TEXT,
    last_sync_at     TEXT,
    last_status      TEXT,
    row_count        INTEGER DEFAULT 0,
    error_message    TEXT
);

-- Sync history (bounded)
CREATE TABLE IF NOT EXISTS sync_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT NOT NULL,
    table_name       TEXT NOT NULL,
    sync_mode        TEXT,
    started_at       TEXT,
    finished_at      TEXT,
    inserted_rows    INTEGER DEFAULT 0,
    updated_rows     INTEGER DEFAULT 0,
    deleted_rows     INTEGER DEFAULT 0,
    status           TEXT,
    error_message    TEXT
);

-- Feature runs (bounded)
CREATE TABLE IF NOT EXISTS feature_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT NOT NULL,
    sql_file         TEXT,
    target_table     TEXT,
    output_path      TEXT,
    output_type      TEXT,
    started_at       TEXT,
    finished_at      TEXT,
    row_count        INTEGER DEFAULT 0,
    status           TEXT,
    error_message    TEXT
);

-- CDC replication state
CREATE TABLE IF NOT EXISTS cdc_state (
    table_name       TEXT PRIMARY KEY,
    slot_name        TEXT,
    publication_name TEXT,
    last_received_lsn TEXT,
    confirmed_lsn    TEXT,
    status           TEXT DEFAULT 'stopped',
    error_message    TEXT,
    updated_at       TEXT DEFAULT (datetime('now'))
);

-- Audit log (bounded)
CREATE TABLE IF NOT EXISTS audit_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp        TEXT DEFAULT (datetime('now')),
    level            TEXT NOT NULL,
    event            TEXT NOT NULL,
    message          TEXT,
    metadata         TEXT
);

-- Watermarks (named progress checkpoints)
CREATE TABLE IF NOT EXISTS watermarks (
    name             TEXT PRIMARY KEY,
    value            TEXT NOT NULL,
    updated_at       TEXT DEFAULT (datetime('now'))
);

-- Lock state (singleton)
CREATE TABLE IF NOT EXISTS lock_state (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    holder_pid       INTEGER,
    holder_host      TEXT,
    acquired_at      TEXT,
    expires_at       TEXT
);

-- Epochs (CDC transactional boundaries)
CREATE TABLE IF NOT EXISTS epochs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    committed_at  TEXT,
    start_lsn     TEXT,
    end_lsn       TEXT,
    row_count     INTEGER NOT NULL DEFAULT 0,
    status        TEXT    NOT NULL DEFAULT 'open'
);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    version          INTEGER NOT NULL DEFAULT 1,
    updated_at       TEXT DEFAULT (datetime('now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sync_history_table ON sync_history(table_name);
CREATE INDEX IF NOT EXISTS idx_sync_history_run ON sync_history(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_feature_runs_run ON feature_runs(run_id);

-- Bounded audit cleanup trigger (collector-agent pattern: keep 1000, delete 100 oldest)
CREATE TRIGGER IF NOT EXISTS audit_cleanup AFTER INSERT ON audit_log
WHEN (SELECT COUNT(*) FROM audit_log) > 1000
BEGIN
    DELETE FROM audit_log WHERE id IN (
        SELECT id FROM audit_log ORDER BY id ASC LIMIT 100
    );
END;

-- Bounded sync_history cleanup trigger
CREATE TRIGGER IF NOT EXISTS sync_history_cleanup AFTER INSERT ON sync_history
WHEN (SELECT COUNT(*) FROM sync_history) > 500
BEGIN
    DELETE FROM sync_history WHERE id IN (
        SELECT id FROM sync_history ORDER BY id ASC LIMIT 50
    );
END;

-- Bounded feature_runs cleanup trigger
CREATE TRIGGER IF NOT EXISTS feature_runs_cleanup AFTER INSERT ON feature_runs
WHEN (SELECT COUNT(*) FROM feature_runs) > 200
BEGIN
    DELETE FROM feature_runs WHERE id IN (
        SELECT id FROM feature_runs ORDER BY id ASC LIMIT 20
    );
END;
`
