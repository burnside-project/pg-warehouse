package models

import "time"

// ProjectIdentity tracks which PG source this project connects to (singleton).
type ProjectIdentity struct {
	ProjectName   string
	PGURLHash     string // SHA-256 of connection URL (never store raw URL)
	WarehousePath string
	CreatedAt     time.Time
}

// CDCState tracks per-table CDC replication state.
type CDCState struct {
	TableName       string
	SlotName        string
	PublicationName string
	ConfirmedLSN    string // pg_lsn as text (e.g. "0/16B3748")
	LastReceivedLSN string
	Status          string // "streaming", "snapshot", "stopped", "error"
	ErrorMessage    string
	UpdatedAt       time.Time
}

// AuditEntry represents a bounded audit log entry.
type AuditEntry struct {
	ID        int64
	Timestamp time.Time
	Level     string // info, warn, error, critical
	Event     string
	Message   string
	Metadata  string // JSON
}

// AuditLevel constants.
const (
	AuditInfo     = "info"
	AuditWarn     = "warn"
	AuditError    = "error"
	AuditCritical = "critical"
)

// Common audit events.
const (
	EventSyncStart     = "sync.start"
	EventSyncComplete  = "sync.complete"
	EventSyncFailure   = "sync.failure"
	EventCDCSetup      = "cdc.setup"
	EventCDCStart      = "cdc.start"
	EventCDCStop       = "cdc.stop"
	EventCDCError      = "cdc.error"
	EventCDCTeardown   = "cdc.teardown"
	EventFeatureRun    = "feature.run"
	EventFeatureError  = "feature.error"
	EventExport        = "export.complete"
	EventInit          = "project.init"
	EventStateMigrated = "state.migrated"
	EventLockAcquired  = "lock.acquired"
	EventLockReleased  = "lock.released"
)

// Watermark represents a named progress checkpoint.
type Watermark struct {
	Name      string
	Value     string
	UpdatedAt time.Time
}

// LockState tracks concurrent execution prevention.
type LockState struct {
	HolderPID  int
	HolderHost string
	AcquiredAt time.Time
	ExpiresAt  time.Time
}
