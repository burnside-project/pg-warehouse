package models

import "time"

// SyncState represents the current sync state for a table.
type SyncState struct {
	TableName       string     `db:"table_name"`
	SyncMode        string     `db:"sync_mode"`
	WatermarkColumn string     `db:"watermark_column"`
	LastWatermark   string     `db:"last_watermark"`
	LastLSN         string     `db:"last_lsn"`
	LastSnapshotAt  *time.Time `db:"last_snapshot_at"`
	LastSyncAt      *time.Time `db:"last_sync_at"`
	LastStatus      string     `db:"last_status"`
	RowCount        int64      `db:"row_count"`
	ErrorMessage    string     `db:"error_message"`
}

// SyncHistory represents a single sync run history entry.
type SyncHistory struct {
	RunID        string     `db:"run_id"`
	TableName    string     `db:"table_name"`
	SyncMode     string     `db:"sync_mode"`
	StartedAt    time.Time  `db:"started_at"`
	FinishedAt   *time.Time `db:"finished_at"`
	InsertedRows int64      `db:"inserted_rows"`
	UpdatedRows  int64      `db:"updated_rows"`
	DeletedRows  int64      `db:"deleted_rows"`
	Status       string     `db:"status"`
	ErrorMessage string     `db:"error_message"`
}

// SyncResult holds the result of a sync operation for one table.
type SyncResult struct {
	TableName    string
	Mode         string
	InsertedRows int64
	UpdatedRows  int64
	Duration     time.Duration
	Error        error
}
