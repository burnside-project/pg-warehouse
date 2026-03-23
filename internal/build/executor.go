package build

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/planner"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/burnside-project/pg-warehouse/internal/registry"
)

// Executor runs a BuildPlan against the warehouse.
type Executor struct {
	silverDB ports.WarehouseStore
	featDB   ports.WarehouseStore
	registry *registry.Registry
	logger   *logging.Logger
}

func NewExecutor(silverDB, featDB ports.WarehouseStore, reg *registry.Registry, logger *logging.Logger) *Executor {
	return &Executor{silverDB: silverDB, featDB: featDB, registry: reg, logger: logger}
}

// Execute runs all steps in the build plan.
func (e *Executor) Execute(ctx context.Context, plan *planner.BuildPlan) error {
	start := time.Now()

	buildID, err := e.registry.StartBuild(ctx, plan.ReleaseName, plan.ReleaseVersion, "", 0, "")
	if err != nil {
		e.logger.Warn("failed to record build start: %v", err)
	}

	var totalRows int64
	for _, step := range plan.Steps {
		e.logger.Info("build step %d/%d: %s -> %s (%s)", step.Order, len(plan.Steps), step.Model.Name, step.TargetTable, step.TargetDB)

		// Read SQL
		content, readErr := os.ReadFile(step.Model.FilePath)
		if readErr != nil {
			e.finishBuild(ctx, buildID, "failed", start, len(plan.Steps), totalRows, readErr.Error())
			return fmt.Errorf("read model %s: %w", step.Model.Name, readErr)
		}

		// Determine target DB
		db := e.silverDB
		if step.TargetDB == "feature" {
			db = e.featDB
		}

		// Execute SQL
		if execErr := db.ExecuteSQL(ctx, string(content)); execErr != nil {
			e.finishBuild(ctx, buildID, "failed", start, len(plan.Steps), totalRows, execErr.Error())
			return fmt.Errorf("execute model %s: %w", step.Model.Name, execErr)
		}

		// Count rows
		count, countErr := db.CountRows(ctx, step.TargetTable)
		if countErr == nil {
			totalRows += count
			e.logger.Info("  %s: %d rows", step.TargetTable, count)
		}
	}

	e.finishBuild(ctx, buildID, "success", start, len(plan.Steps), totalRows, "")
	return nil
}

func (e *Executor) finishBuild(ctx context.Context, buildID int64, status string, start time.Time, modelCount int, rowCount int64, errMsg string) {
	durationMs := time.Since(start).Milliseconds()
	if finishErr := e.registry.FinishBuild(ctx, buildID, status, durationMs, modelCount, rowCount, errMsg); finishErr != nil {
		e.logger.Warn("failed to record build finish: %v", finishErr)
	}
}
