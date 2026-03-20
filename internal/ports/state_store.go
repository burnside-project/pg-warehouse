package ports

import (
	"context"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// StateStore extends MetadataStore with platform state management capabilities.
// It provides audit logging, CDC state tracking, watermarks, and execution locking.
type StateStore interface {
	// Embed MetadataStore for backward compatibility with existing services.
	MetadataStore

	// Project identity
	GetProjectIdentity(ctx context.Context) (*models.ProjectIdentity, error)
	SaveProjectIdentity(ctx context.Context, id *models.ProjectIdentity) error

	// CDC state (per-table)
	GetCDCState(ctx context.Context, table string) (*models.CDCState, error)
	GetAllCDCStates(ctx context.Context) ([]models.CDCState, error)
	UpsertCDCState(ctx context.Context, state *models.CDCState) error

	// Audit log (bounded, collector-agent pattern)
	AddAuditEntry(ctx context.Context, level, event, message string, metadata map[string]any) error
	GetRecentAuditEntries(ctx context.Context, limit int) ([]models.AuditEntry, error)

	// Watermarks (named progress checkpoints)
	GetWatermark(ctx context.Context, name string) (*models.Watermark, error)
	SetWatermark(ctx context.Context, name string, value string) error

	// Lock (prevent concurrent pg-warehouse execution)
	TryAcquireLock(ctx context.Context, pid int, hostname string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context) error
	GetLockState(ctx context.Context) (*models.LockState, error)

	// Schema version
	SchemaVersion(ctx context.Context) (int, error)

	// Epoch management (CDC transactional boundaries)
	OpenEpoch(ctx context.Context) (*models.Epoch, error)
	CommitEpoch(ctx context.Context, epochID int64, endLSN string, rowCount int64) error
	MarkEpochMerged(ctx context.Context, epochID int64) error
	GetOpenEpoch(ctx context.Context) (*models.Epoch, error)
	GetCommittedEpochs(ctx context.Context) ([]models.Epoch, error)
	GetLatestMergedEpoch(ctx context.Context) (*models.Epoch, error)

	// Lifecycle
	Close() error
}
