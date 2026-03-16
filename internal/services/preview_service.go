package services

import (
	"context"
	"fmt"
	"os"

	"github.com/burnside-project/pg-warehouse/internal/domain/feature"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// PreviewService handles SQL preview execution.
type PreviewService struct {
	warehouse ports.WarehouseStore
	logger    *logging.Logger
}

// NewPreviewService creates a new PreviewService.
func NewPreviewService(wh ports.WarehouseStore, logger *logging.Logger) *PreviewService {
	return &PreviewService{
		warehouse: wh,
		logger:    logger,
	}
}

// Preview executes a SQL file and returns sample rows without persisting output.
func (s *PreviewService) Preview(ctx context.Context, sqlFile string, limit int) ([]map[string]any, error) {
	if err := feature.ValidateSQLFile(sqlFile); err != nil {
		return nil, err
	}

	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	if limit <= 0 {
		limit = 20
	}

	rows, err := s.warehouse.QueryRows(ctx, string(sqlBytes), limit)
	if err != nil {
		return nil, fmt.Errorf("preview query failed: %w", err)
	}

	return rows, nil
}
