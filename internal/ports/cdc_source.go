package ports

import (
	"context"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// CDCEvent represents a single change data capture event from PostgreSQL.
type CDCEvent struct {
	Table     string
	Operation string // "INSERT", "UPDATE", "DELETE"
	LSN       string
	OldTuple  map[string]any // nil for INSERT
	NewTuple  map[string]any // nil for DELETE
	Timestamp time.Time
}

// CDCStatus represents the current status of a CDC replication slot.
type CDCStatus struct {
	SlotName     string
	Plugin       string
	Active       bool
	ConfirmedLSN string
	CurrentLSN   string
	LagBytes     int64
}

// CDCSource defines the contract for PostgreSQL logical replication.
type CDCSource interface {
	// Setup creates a publication and replication slot on the source PostgreSQL.
	Setup(ctx context.Context, tables []string, publicationName string, slotName string) error

	// Teardown drops the publication and replication slot.
	Teardown(ctx context.Context, publicationName string, slotName string) error

	// StartSnapshot performs initial consistent table copy before streaming.
	// Returns rows, column info, and the snapshot LSN.
	StartSnapshot(ctx context.Context, table string) ([]map[string]any, []models.ColumnInfo, string, error)

	// Stream starts logical replication from the given LSN, sending events to the channel.
	// Blocks until ctx is cancelled or an error occurs.
	Stream(ctx context.Context, slotName string, publicationName string, startLSN string, events chan<- CDCEvent) error

	// ConfirmLSN sends a standby status update to advance the replication slot position.
	ConfirmLSN(ctx context.Context, lsn string) error

	// Status returns the current replication slot status.
	Status(ctx context.Context, slotName string) (*CDCStatus, error)

	// Close releases the CDC connection resources.
	Close() error
}
