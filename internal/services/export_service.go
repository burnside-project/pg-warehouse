package services

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/domain/feature"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/burnside-project/pg-warehouse/internal/util"
)

// ExportService handles table exports.
type ExportService struct {
	warehouse ports.WarehouseStore
	logger    *logging.Logger
}

// NewExportService creates a new ExportService.
func NewExportService(wh ports.WarehouseStore, logger *logging.Logger) *ExportService {
	return &ExportService{
		warehouse: wh,
		logger:    logger,
	}
}

// Export exports a warehouse table to the specified file.
func (s *ExportService) Export(ctx context.Context, table string, outputPath string, fileType string) error {
	if err := feature.ValidateOutputType(fileType); err != nil {
		return err
	}

	exists, err := s.warehouse.TableExists(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}

	if err := util.EnsureDir(filepath.Dir(outputPath)); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	s.logger.Info("exporting %s to %s (%s)", table, outputPath, fileType)
	if err := s.warehouse.ExportTable(ctx, table, outputPath, fileType); err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	rowCount, _ := s.warehouse.CountRows(ctx, table)
	s.logger.Info("exported %d rows to %s", rowCount, outputPath)
	return nil
}
