package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/domain/feature"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/burnside-project/pg-warehouse/internal/util"
)

// RunService handles SQL feature job execution.
type RunService struct {
	warehouse ports.WarehouseStore
	metadata  ports.MetadataStore
	logger    *logging.Logger
}

// NewRunService creates a new RunService.
func NewRunService(wh ports.WarehouseStore, meta ports.MetadataStore, logger *logging.Logger) *RunService {
	return &RunService{
		warehouse: wh,
		metadata:  meta,
		logger:    logger,
	}
}

// Run executes a SQL feature file, validates the target, and optionally exports.
func (s *RunService) Run(ctx context.Context, sqlFile string, targetTable string, outputPath string, outputType string) error {
	// Validate inputs
	if err := feature.ValidateSQLFile(sqlFile); err != nil {
		return err
	}
	if err := feature.ValidateTargetTable(targetTable); err != nil {
		return err
	}
	if outputPath != "" {
		if err := feature.ValidateOutputType(outputType); err != nil {
			return err
		}
	}

	// Read SQL file
	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}
	sqlContent := string(sqlBytes)

	runID := util.NewRunID()
	startTime := time.Now().UTC()

	// Record feature run start
	run := &models.FeatureRun{
		RunID:       runID,
		SQLFile:     sqlFile,
		TargetTable: targetTable,
		OutputPath:  outputPath,
		OutputType:  outputType,
		StartedAt:   startTime,
		Status:      "running",
	}
	_ = s.metadata.InsertFeatureRun(ctx, run)

	// Execute SQL
	s.logger.Info("executing SQL file: %s", sqlFile)
	if err := s.warehouse.ExecuteSQL(ctx, sqlContent); err != nil {
		s.finalizeRun(ctx, run, 0, "failed", err.Error())
		return fmt.Errorf("failed to execute SQL: %w", err)
	}

	// Validate target table exists
	exists, err := s.warehouse.TableExists(ctx, targetTable)
	if err != nil {
		s.finalizeRun(ctx, run, 0, "failed", err.Error())
		return fmt.Errorf("failed to check target table: %w", err)
	}
	if !exists {
		errMsg := fmt.Sprintf("target table %s was not created by SQL file", targetTable)
		s.finalizeRun(ctx, run, 0, "failed", errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	// Count rows
	rowCount, err := s.warehouse.CountRows(ctx, targetTable)
	if err != nil {
		s.finalizeRun(ctx, run, 0, "failed", err.Error())
		return fmt.Errorf("failed to count rows: %w", err)
	}
	s.logger.Info("target table %s: %d rows", targetTable, rowCount)

	// Export if output path specified
	if outputPath != "" {
		if err := util.EnsureDir(filepath.Dir(outputPath)); err != nil {
			s.finalizeRun(ctx, run, rowCount, "failed", err.Error())
			return fmt.Errorf("failed to create output directory: %w", err)
		}
		s.logger.Info("exporting to %s (%s)", outputPath, outputType)
		if err := s.warehouse.ExportTable(ctx, targetTable, outputPath, outputType); err != nil {
			s.finalizeRun(ctx, run, rowCount, "failed", err.Error())
			return fmt.Errorf("failed to export: %w", err)
		}
	}

	s.finalizeRun(ctx, run, rowCount, "success", "")
	s.logger.Info("feature run complete: %s -> %s (%d rows)", sqlFile, targetTable, rowCount)
	return nil
}

func (s *RunService) finalizeRun(ctx context.Context, run *models.FeatureRun, rowCount int64, status string, errMsg string) {
	now := time.Now().UTC()
	run.FinishedAt = &now
	run.RowCount = rowCount
	run.Status = status
	run.ErrorMessage = errMsg
	_ = s.metadata.UpdateFeatureRun(ctx, run)
}
