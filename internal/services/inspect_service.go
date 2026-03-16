package services

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// InspectService handles warehouse inspection.
type InspectService struct {
	inspector ports.Inspector
	metadata  ports.MetadataStore
}

// NewInspectService creates a new InspectService.
func NewInspectService(inspector ports.Inspector, metadata ports.MetadataStore) *InspectService {
	return &InspectService{
		inspector: inspector,
		metadata:  metadata,
	}
}

// ListTables returns all tables in the warehouse.
func (s *InspectService) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	return s.inspector.ListTables(ctx)
}

// DescribeTable returns the schema for a specific table.
func (s *InspectService) DescribeTable(ctx context.Context, table string) (*models.TableSchema, error) {
	return s.inspector.DescribeTable(ctx, table)
}

// GetSyncState returns the current sync state for all tables.
func (s *InspectService) GetSyncState(ctx context.Context) ([]models.SyncState, error) {
	return s.metadata.GetAllSyncStates(ctx)
}
