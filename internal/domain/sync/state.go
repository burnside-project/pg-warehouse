package sync

import "time"

// SyncMode represents the mode of synchronization
type SyncMode string

const (
	SyncModeFull        SyncMode = "full"
	SyncModeIncremental SyncMode = "incremental"
)

// SyncStatus represents the status of a sync operation
type SyncStatus string

const (
	SyncStatusPending  SyncStatus = "pending"
	SyncStatusRunning  SyncStatus = "running"
	SyncStatusSuccess  SyncStatus = "success"
	SyncStatusFailed   SyncStatus = "failed"
)

// State holds the current sync state for a table
type State struct {
	TableName       string
	SyncMode        SyncMode
	WatermarkColumn string
	LastWatermark   string
	LastLSN         string
	LastSnapshotAt  *time.Time
	LastSyncAt      *time.Time
	LastStatus      SyncStatus
	RowCount        int64
	ErrorMessage    string
}

// History records a single sync run
type History struct {
	RunID        string
	TableName    string
	SyncMode     SyncMode
	StartedAt    time.Time
	FinishedAt   *time.Time
	InsertedRows int64
	UpdatedRows  int64
	DeletedRows  int64
	Status       SyncStatus
	ErrorMessage string
}