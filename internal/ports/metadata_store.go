package ports

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// MetadataStore defines the contract for reading and writing sync/feature metadata.
type MetadataStore interface {
	// GetSyncState returns the current sync state for a table.
	GetSyncState(ctx context.Context, table string) (*models.SyncState, error)

	// GetAllSyncStates returns sync state for all tables.
	GetAllSyncStates(ctx context.Context) ([]models.SyncState, error)

	// UpsertSyncState creates or updates the sync state for a table.
	UpsertSyncState(ctx context.Context, state *models.SyncState) error

	// InsertSyncHistory records a sync history entry.
	InsertSyncHistory(ctx context.Context, history *models.SyncHistory) error

	// InsertFeatureRun records a feature run entry.
	InsertFeatureRun(ctx context.Context, run *models.FeatureRun) error

	// UpdateFeatureRun updates an existing feature run entry.
	UpdateFeatureRun(ctx context.Context, run *models.FeatureRun) error
}
