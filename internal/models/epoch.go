package models

import "time"

// Epoch represents a transactional boundary for CDC writes.
// CDC stamps every row with an _epoch column. Epochs are committed
// at regular intervals, then merged from stage to raw.
type Epoch struct {
	ID          int64
	StartedAt   time.Time
	CommittedAt *time.Time
	StartLSN    string
	EndLSN      string
	RowCount    int64
	Status      string // "open", "committed", "merged"
}

const (
	EpochStatusOpen      = "open"
	EpochStatusCommitted = "committed"
	EpochStatusMerged    = "merged"
)
