package build

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/planner"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/burnside-project/pg-warehouse/internal/registry"
)

var (
	refPattern    = regexp.MustCompile(`\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}`)
	sourcePattern = regexp.MustCompile(`\{\{\s*source\(\s*'([^']+)'\s*(?:,\s*'([^']+)')?\s*\)\s*\}\}`)
	createTableRe = regexp.MustCompile(`(?i)(CREATE\s+OR\s+REPLACE\s+TABLE\s+)(?:(\w+)\.)?(\w+)(\s+AS\b)`)
)

// resolveSQL replaces {{ ref('X') }} and {{ source('S', 'T') }} with actual table names,
// and rewrites CREATE TABLE to target the correct schema.
func resolveSQL(sql string, targetSchema string, sourceSchema string) string {
	// Replace {{ ref('model') }} → sourceSchema.model (reads from same or upstream layer)
	resolved := refPattern.ReplaceAllString(sql, sourceSchema+".$1")

	// Replace {{ source('schema', 'table') }} → sourceSchema.table
	resolved = sourcePattern.ReplaceAllStringFunc(resolved, func(match string) string {
		sub := sourcePattern.FindStringSubmatch(match)
		if len(sub) >= 3 && sub[2] != "" {
			return sourceSchema + "." + sub[2]
		}
		if len(sub) >= 2 {
			return sourceSchema + "." + sub[1]
		}
		return match
	})

	// Rewrite CREATE TABLE to target schema
	resolved = createTableRe.ReplaceAllString(resolved, "${1}"+targetSchema+".${3}${4}")

	return resolved
}

// Executor runs a BuildPlan against the warehouse.
type Executor struct {
	silverDB   ports.WarehouseStore
	featDB     ports.WarehouseStore
	silverPath string // path to silver.duckdb for ATTACH from feature
	registry   *registry.Registry
	logger     *logging.Logger
}

func NewExecutor(silverDB, featDB ports.WarehouseStore, silverPath string, reg *registry.Registry, logger *logging.Logger) *Executor {
	return &Executor{silverDB: silverDB, featDB: featDB, silverPath: silverPath, registry: reg, logger: logger}
}

// Execute runs all steps in the build plan.
func (e *Executor) Execute(ctx context.Context, plan *planner.BuildPlan) error {
	start := time.Now()

	buildID, err := e.registry.StartBuild(ctx, plan.ReleaseName, plan.ReleaseVersion, "", 0, "")
	if err != nil {
		e.logger.Warn("failed to record build start: %v", err)
	}

	var totalRows int64
	featureRefreshed := false

	for _, step := range plan.Steps {
		// Before the first feature step, refresh feature v0 from silver v1
		if step.TargetDB == "feature" && !featureRefreshed && e.silverPath != "" {
			featureRefreshed = true
			e.refreshFeatureV0(ctx)
		}

		e.logger.Info("build step %d/%d: %s -> %s (%s)", step.Order, len(plan.Steps), step.Model.Name, step.TargetTable, step.TargetDB)

		// Read SQL
		content, readErr := os.ReadFile(step.Model.FilePath)
		if readErr != nil {
			e.finishBuild(ctx, buildID, "failed", start, len(plan.Steps), totalRows, readErr.Error())
			return fmt.Errorf("read model %s: %w", step.Model.Name, readErr)
		}

		// Determine target DB and source schema
		db := e.silverDB
		sourceSchema := "v0"
		targetSchema := "v1"
		if step.TargetDB == "feature" {
			db = e.featDB
			// Feature models read from v0 in feature.duckdb (silver data)
		}

		// Resolve ref(), source(), and rewrite CREATE TABLE
		resolved := resolveSQL(string(content), targetSchema, sourceSchema)

		// Ensure target schema exists
		_ = db.ExecuteSQL(ctx, "CREATE SCHEMA IF NOT EXISTS "+targetSchema)

		// Execute resolved SQL
		if execErr := db.ExecuteSQL(ctx, resolved); execErr != nil {
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

// refreshFeatureV0 copies silver v1 tables into feature v0 via ATTACH.
func (e *Executor) refreshFeatureV0(ctx context.Context) {
	e.logger.Info("refreshing feature v0 from silver v1...")
	attacher, ok := e.featDB.(interface {
		AttachReadOnly(ctx context.Context, path string, alias string) error
		DetachDatabase(ctx context.Context, alias string) error
	})
	if !ok {
		e.logger.Warn("feature DB does not support ATTACH")
		return
	}

	if attachErr := attacher.AttachReadOnly(ctx, e.silverPath, "silver_src"); attachErr != nil {
		e.logger.Warn("failed to attach silver for feature refresh: %v", attachErr)
		return
	}

	rows, listErr := e.silverDB.QueryRows(ctx,
		"SELECT table_name FROM duckdb_tables() WHERE schema_name = 'v1' ORDER BY table_name", 100)
	if listErr != nil {
		_ = attacher.DetachDatabase(ctx, "silver_src")
		return
	}

	_ = e.featDB.ExecuteSQL(ctx, "CREATE SCHEMA IF NOT EXISTS v0")
	for _, row := range rows {
		tableName := fmt.Sprintf("%v", row["table_name"])
		copySQL := fmt.Sprintf(
			"CREATE OR REPLACE TABLE v0.\"%s\" AS SELECT * FROM silver_src.v1.\"%s\"",
			tableName, tableName)
		_ = e.featDB.ExecuteSQL(ctx, copySQL)
	}
	_ = attacher.DetachDatabase(ctx, "silver_src")
	e.logger.Info("feature v0 refreshed (%d tables from silver v1)", len(rows))
}

func (e *Executor) finishBuild(ctx context.Context, buildID int64, status string, start time.Time, modelCount int, rowCount int64, errMsg string) {
	durationMs := time.Since(start).Milliseconds()
	if finishErr := e.registry.FinishBuild(ctx, buildID, status, durationMs, modelCount, rowCount, errMsg); finishErr != nil {
		e.logger.Warn("failed to record build finish: %v", finishErr)
	}
}
