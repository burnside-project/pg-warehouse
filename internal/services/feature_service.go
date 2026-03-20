package services

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/burnside-project/pg-warehouse/internal/util"
)

// FeatureService runs feat SQL against feature.duckdb with silver.duckdb attached read-only.
type FeatureService struct {
	feature    ports.WarehouseStore
	silverPath string
	metadata   ports.MetadataStore
	logger     *logging.Logger
}

// NewFeatureService creates a new FeatureService.
func NewFeatureService(feature ports.WarehouseStore, silverPath string, metadata ports.MetadataStore, logger *logging.Logger) *FeatureService {
	return &FeatureService{
		feature:    feature,
		silverPath: silverPath,
		metadata:   metadata,
		logger:     logger,
	}
}

// Run executes a feat SQL file against feature.duckdb, attaching silver.duckdb as read-only source.
func (s *FeatureService) Run(ctx context.Context, sqlFile, targetTable, outputPath, outputType string) (int64, error) {
	// Read SQL
	content, err := os.ReadFile(sqlFile)
	if err != nil {
		return 0, fmt.Errorf("read SQL file: %w", err)
	}

	// Record run
	run := &models.FeatureRun{
		RunID:       util.NewRunID(),
		SQLFile:     sqlFile,
		TargetTable: targetTable,
		OutputPath:  outputPath,
		OutputType:  outputType,
		Status:      "running",
		StartedAt:   time.Now(),
	}
	if err := s.metadata.InsertFeatureRun(ctx, run); err != nil {
		s.logger.Warn("Failed to record feature run: %v", err)
	}

	// Attach silver read-only
	if s.silverPath != "" {
		if attacher, ok := s.feature.(interface {
			AttachReadOnly(ctx context.Context, path string, alias string) error
		}); ok {
			if err := attacher.AttachReadOnly(ctx, s.silverPath, "silver"); err != nil {
				return 0, fmt.Errorf("attach silver: %w", err)
			}
			defer func() {
				if detacher, ok := s.feature.(interface {
					DetachDatabase(ctx context.Context, alias string) error
				}); ok {
					detacher.DetachDatabase(ctx, "silver")
				}
			}()
		}
	}

	// Execute SQL
	if err := s.feature.ExecuteSQL(ctx, string(content)); err != nil {
		run.Status = "failed"
		run.ErrorMessage = err.Error()
		s.metadata.UpdateFeatureRun(ctx, run)
		return 0, fmt.Errorf("execute SQL: %w", err)
	}

	// Count rows
	rowCount, err := s.feature.CountRows(ctx, targetTable)
	if err != nil {
		s.logger.Warn("Failed to count rows: %v", err)
	}

	// Export if requested
	if outputPath != "" && outputType != "" {
		if err := s.feature.ExportTable(ctx, targetTable, outputPath, outputType); err != nil {
			run.Status = "failed"
			run.ErrorMessage = err.Error()
			s.metadata.UpdateFeatureRun(ctx, run)
			return rowCount, fmt.Errorf("export: %w", err)
		}
	}

	// Finalize
	run.RowCount = rowCount
	run.Status = "success"
	now := time.Now()
	run.FinishedAt = &now
	s.metadata.UpdateFeatureRun(ctx, run)

	return rowCount, nil
}
