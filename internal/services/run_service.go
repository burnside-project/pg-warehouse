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

// SchemaManager is the subset of Warehouse used for SET schema.
type SchemaManager interface {
	SetSchema(ctx context.Context, schema string) error
}

// RunService handles SQL feature job execution.
type RunService struct {
	warehouse ports.WarehouseStore
	metadata  ports.MetadataStore
	logger    *logging.Logger

	// Multi-DB mode fields
	multiMode      bool
	sourcePath     string // path to ATTACH as read-only source
	sourceAlias    string // alias used in ATTACH

	// Schema rewriting fields
	sourceSchema string // source schema for SET schema (e.g., "v0")
}

// NewRunService creates a new RunService (single-file mode).
func NewRunService(wh ports.WarehouseStore, meta ports.MetadataStore, logger *logging.Logger) *RunService {
	return &RunService{
		warehouse: wh,
		metadata:  meta,
		logger:    logger,
	}
}

// MultiDBAttacher is the subset of Warehouse used for ATTACH / DETACH.
type MultiDBAttacher interface {
	AttachReadOnly(ctx context.Context, path string, alias string) error
	DetachDatabase(ctx context.Context, alias string) error
}

// NewRunServiceMulti creates a RunService that operates in multi-DB mode.
// sourcePath is the DuckDB file to ATTACH READ_ONLY before executing SQL.
// sourceAlias is the alias that SQL files should reference (e.g. "warehouse" or "silver").
func NewRunServiceMulti(wh ports.WarehouseStore, meta ports.MetadataStore, logger *logging.Logger, sourcePath string, sourceAlias string) *RunService {
	return &RunService{
		warehouse:   wh,
		metadata:    meta,
		logger:      logger,
		multiMode:   true,
		sourcePath:  sourcePath,
		sourceAlias: sourceAlias,
	}
}

// WithSourceSchema returns a copy of the RunService with the source schema set.
// When set, SET schema = '<sourceSchema>' is executed before SQL to resolve
// unqualified table references from the correct layer.
func (s *RunService) WithSourceSchema(schema string) *RunService {
	cp := *s
	cp.sourceSchema = schema
	return &cp
}

// Run executes a SQL feature file, validates the target, and optionally exports.
// Returns the row count of the target table on success.
func (s *RunService) Run(ctx context.Context, sqlFile string, targetTable string, outputPath string, outputType string) (int64, error) {
	// Validate inputs
	if err := feature.ValidateSQLFile(sqlFile); err != nil {
		return 0, err
	}
	if err := feature.ValidateTargetTable(targetTable); err != nil {
		return 0, err
	}
	if outputPath != "" {
		if err := feature.ValidateOutputType(outputType); err != nil {
			return 0, err
		}
	}

	// Read SQL file
	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read SQL file: %w", err)
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

	// In multi-DB mode, attach the source database before execution.
	if s.multiMode {
		if attacher, ok := s.warehouse.(MultiDBAttacher); ok {
			s.logger.Info("attaching %s as %s (READ_ONLY)", s.sourcePath, s.sourceAlias)
			if err := attacher.AttachReadOnly(ctx, s.sourcePath, s.sourceAlias); err != nil {
				s.finalizeRun(ctx, run, 0, "failed", err.Error())
				return 0, fmt.Errorf("failed to attach source db: %w", err)
			}
			defer func() {
				s.logger.Info("detaching %s", s.sourceAlias)
				_ = attacher.DetachDatabase(ctx, s.sourceAlias)
			}()
		}
	}

	// Set source schema for unqualified table resolution
	if s.sourceSchema != "" {
		if sm, ok := s.warehouse.(SchemaManager); ok {
			s.logger.Info("setting source schema: %s", s.sourceSchema)
			if err := sm.SetSchema(ctx, s.sourceSchema); err != nil {
				s.finalizeRun(ctx, run, 0, "failed", err.Error())
				return 0, fmt.Errorf("failed to set source schema: %w", err)
			}
		}
	}

	// Execute SQL
	s.logger.Info("executing SQL file: %s", sqlFile)
	if err := s.warehouse.ExecuteSQL(ctx, sqlContent); err != nil {
		s.finalizeRun(ctx, run, 0, "failed", err.Error())
		return 0, fmt.Errorf("failed to execute SQL: %w", err)
	}

	// Validate target table exists
	exists, err := s.warehouse.TableExists(ctx, targetTable)
	if err != nil {
		s.finalizeRun(ctx, run, 0, "failed", err.Error())
		return 0, fmt.Errorf("failed to check target table: %w", err)
	}
	if !exists {
		errMsg := fmt.Sprintf("target table %s was not created by SQL file", targetTable)
		s.finalizeRun(ctx, run, 0, "failed", errMsg)
		return 0, fmt.Errorf("%s", errMsg)
	}

	// Count rows
	rowCount, err := s.warehouse.CountRows(ctx, targetTable)
	if err != nil {
		s.finalizeRun(ctx, run, 0, "failed", err.Error())
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	s.logger.Info("target table %s: %d rows", targetTable, rowCount)

	// Export if output path specified
	if outputPath != "" {
		if err := util.EnsureDir(filepath.Dir(outputPath)); err != nil {
			s.finalizeRun(ctx, run, rowCount, "failed", err.Error())
			return 0, fmt.Errorf("failed to create output directory: %w", err)
		}
		s.logger.Info("exporting to %s (%s)", outputPath, outputType)
		if err := s.warehouse.ExportTable(ctx, targetTable, outputPath, outputType); err != nil {
			s.finalizeRun(ctx, run, rowCount, "failed", err.Error())
			return 0, fmt.Errorf("failed to export: %w", err)
		}
	}

	s.finalizeRun(ctx, run, rowCount, "success", "")
	s.logger.Info("feature run complete: %s -> %s (%d rows)", sqlFile, targetTable, rowCount)
	return rowCount, nil
}

func (s *RunService) finalizeRun(ctx context.Context, run *models.FeatureRun, rowCount int64, status string, errMsg string) {
	now := time.Now().UTC()
	run.FinishedAt = &now
	run.RowCount = rowCount
	run.Status = status
	run.ErrorMessage = errMsg
	_ = s.metadata.UpdateFeatureRun(ctx, run)
}
