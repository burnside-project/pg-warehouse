package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/domain/feature"
	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

// createTableRe matches CREATE OR REPLACE TABLE [schema.]table AS
// Group 1 = optional schema, Group 2 = table name
var createTableRe = regexp.MustCompile(
	`(?i)(CREATE\s+OR\s+REPLACE\s+TABLE\s+)(?:(\w+)\.)?(\w+)(\s+AS\b)`,
)

// isFeatTarget returns true when the target table belongs to the feat schema.
func isFeatTarget(table string) bool {
	return strings.HasPrefix(strings.ToLower(table), "feat.")
}

// isFeatDir returns true if the directory path looks like a feature layer directory.
func isFeatDir(dir string) bool {
	d := strings.ToLower(filepath.Clean(dir))
	return strings.Contains(d, "feat")
}

// stripNumericPrefix removes a leading NNN_ prefix from a filename stem.
// 001_order_enriched → order_enriched
func stripNumericPrefix(name string) string {
	for i, c := range name {
		if c == '_' {
			allDigits := true
			for _, d := range name[:i] {
				if d < '0' || d > '9' {
					allDigits = false
					break
				}
			}
			if allDigits {
				return name[i+1:]
			}
		}
	}
	return name
}

// deriveTargetFromFilename extracts the target table from a SQL filename.
// sql/silver/v1/001_order_enriched.sql → v1.order_enriched
// sql/silver/001_order_enriched.sql    → v1.order_enriched (defaults to v1 for silver)
// sql/feat/001_sales_summary.sql       → v1.sales_summary (defaults to v1 for feat)
func deriveTargetFromFilename(sqlFile string) string {
	base := filepath.Base(sqlFile)
	name := strings.TrimSuffix(base, ".sql")
	name = stripNumericPrefix(name)

	dir := filepath.Base(filepath.Dir(sqlFile))
	if dir == "silver" || dir == "feat" {
		dir = "v1"
	}

	return dir + "." + name
}

// deriveTargetSchema extracts the target schema from a --sql-dir path.
// sql/silver/v1/ → v1, sql/silver/v2/ → v2, sql/silver/ → v1, sql/feat/ → v1
func deriveTargetSchema(dir string) string {
	base := filepath.Base(filepath.Clean(dir))
	// If directory is a version directory (v1, v2, ...), use it directly
	if strings.HasPrefix(base, "v") && len(base) > 1 {
		allDigits := true
		for _, c := range base[1:] {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			return base
		}
	}
	// Default: silver/ and feat/ both target v1
	return "v1"
}

// deriveDuckDBLayer determines which DuckDB file to target based on directory path.
// Returns "silver" or "feature".
func deriveDuckDBLayer(dir string) string {
	if isFeatDir(dir) {
		return "feature"
	}
	return "silver"
}

// rewriteSQL rewrites generic SQL for schema-qualified execution.
// 1. Rewrites CREATE OR REPLACE TABLE [schema.]name AS → CREATE OR REPLACE TABLE target.name AS
// 2. Returns the rewritten SQL (caller sets search_path separately via SetSchema).
func rewriteSQL(sql string, targetSchema string) string {
	return createTableRe.ReplaceAllString(sql,
		"${1}"+targetSchema+".${3}${4}")
}

// validateTargetAccess checks that the target schema is not reserved.
// Returns an error with a DANGER message if access is blocked.
func validateTargetAccess(targetSchema string) error {
	return feature.ValidateTargetSchema(targetSchema)
}

var (
	runSQLFile       string
	runSQLDir        string
	runTargetTable   string
	runTargetSchema  string
	runOutputPath    string
	runFileType      string
	runRefresh       bool
	runPipeline      bool
	runPromote       bool
	runVersion       int
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run SQL feature job",
	Long: `Run SQL transformations against the warehouse.

Modes:
  --refresh        Snapshot raw.duckdb into silver.duckdb v0 (refresh layer)
  --pipeline       Discover and run all SQL in sql/silver/ and sql/feat/
  --sql-dir DIR    Run all SQL files in a directory (sorted by numeric prefix)
  --sql-file FILE  Run a single SQL file
  --promote        Swap current.* views to point at the specified --version

Schema wiring (for --sql-dir and --pipeline):
  SQL files are generic — no schema prefixes needed. pg-warehouse sets the
  source schema (v0) for reads and rewrites CREATE TABLE to the target schema.

  --target-schema  Override the target schema (default: derived from directory)

Reserved schemas (DANGER — will be rejected):
  raw, stage, v0, _meta — these are managed by pg-warehouse internally.

Examples:
  pg-warehouse run --refresh --pipeline --promote
  pg-warehouse run --sql-dir ./sql/silver/
  pg-warehouse run --sql-dir ./sql/feat/ --target-schema v2
  pg-warehouse run --sql-file ./sql/silver/001_order_enriched.sql`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !runRefresh && !runPipeline && !runPromote && runSQLFile == "" && runSQLDir == "" {
			return cmd.Help()
		}

		// Validate --target-schema if provided
		if runTargetSchema != "" {
			if err := validateTargetAccess(runTargetSchema); err != nil {
				ui.Danger(err.Error())
				return err
			}
		}

		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// --refresh: snapshot raw into silver v0
		if runRefresh {
			if err := doRefresh(ctx, app); err != nil {
				return err
			}
		}

		// --pipeline: discover and run all SQL files
		if runPipeline {
			if err := doPipeline(ctx, app); err != nil {
				return err
			}
		}

		// --sql-dir: run all SQL files in a directory
		if runSQLDir != "" {
			if err := doSQLDir(ctx, app); err != nil {
				return err
			}
		}

		// --promote: swap current.* views
		if runPromote {
			if err := doPromote(ctx, app); err != nil {
				return err
			}
		}

		// --sql-file: run a single file
		if runSQLFile != "" {
			if err := doSingleRun(ctx, app); err != nil {
				return err
			}
		}

		return nil
	},
}

// doRefresh snapshots raw.duckdb into silver.duckdb v0.
func doRefresh(ctx context.Context, app *App) error {
	silverDB := app.SilverDB()
	rawPath := app.Cfg.DuckDB.Raw
	if rawPath == "" {
		return fmt.Errorf("--refresh requires multi-file mode (duckdb.raw must be set)")
	}

	svc := services.NewRefreshService(silverDB, app.Logger)
	ui.Info("Refreshing: snapshotting raw.duckdb into silver.duckdb v0...")
	if err := svc.Refresh(ctx, rawPath, "raw"); err != nil {
		return fmt.Errorf("refresh failed: %w", err)
	}
	ui.Success("Refresh complete")
	return nil
}

// doPipeline discovers SQL files in sql/silver/ and sql/feat/, then runs each.
func doPipeline(ctx context.Context, app *App) error {
	// Phase 1: silver
	silverDir := "sql/silver/v1"
	if _, err := os.Stat(silverDir); os.IsNotExist(err) {
		silverDir = "sql/silver"
	}
	if err := runDir(ctx, app, silverDir, "v1", "v0", false); err != nil {
		return err
	}

	// Phase 2: feat
	if err := runDir(ctx, app, "sql/feat", "v1", "v0", true); err != nil {
		return err
	}

	ui.Success("Pipeline complete")
	return nil
}

// doSQLDir runs all SQL files in the specified directory.
func doSQLDir(ctx context.Context, app *App) error {
	targetSchema := runTargetSchema
	if targetSchema == "" {
		targetSchema = deriveTargetSchema(runSQLDir)
	}

	if err := validateTargetAccess(targetSchema); err != nil {
		ui.Danger(err.Error())
		return err
	}

	export := isFeatDir(runSQLDir)
	return runDir(ctx, app, runSQLDir, targetSchema, "v0", export)
}

// runDir discovers *.sql files in dir, sorts by name, rewrites SQL, and runs each.
func runDir(ctx context.Context, app *App, dir string, targetSchema string, sourceSchema string, export bool) error {
	pattern := filepath.Join(dir, "*.sql")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob %s: %w", pattern, err)
	}
	if len(files) == 0 {
		ui.Warn(fmt.Sprintf("No SQL files found in %s", dir))
		return nil
	}
	sort.Strings(files)

	fileType := runFileType
	if fileType == "" && app.Cfg.Run.DefaultFileType != "" {
		fileType = app.Cfg.Run.DefaultFileType
	}
	if fileType == "" {
		fileType = "parquet"
	}

	layer := deriveDuckDBLayer(dir)

	for _, sqlFile := range files {
		// Derive table name from filename
		base := filepath.Base(sqlFile)
		tableName := strings.TrimSuffix(base, ".sql")
		tableName = stripNumericPrefix(tableName)
		target := targetSchema + "." + tableName

		ui.Info(fmt.Sprintf("Pipeline [%s]: %s -> %s (source: %s)", layer, sqlFile, target, sourceSchema))

		// Read SQL file
		sqlBytes, readErr := os.ReadFile(sqlFile)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", sqlFile, readErr)
		}

		// Rewrite SQL: qualify CREATE TABLE with target schema
		rewritten := rewriteSQL(string(sqlBytes), targetSchema)

		// Write rewritten SQL to temp file for RunService
		tmpFile, tmpErr := os.CreateTemp("", "pgwh-*.sql")
		if tmpErr != nil {
			return fmt.Errorf("create temp file: %w", tmpErr)
		}
		if _, writeErr := tmpFile.WriteString(rewritten); writeErr != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return fmt.Errorf("write temp file: %w", writeErr)
		}
		tmpFile.Close()

		// Build RunService with source schema
		svc := buildRunService(app, target).WithSourceSchema(sourceSchema)

		// Derive output path for export
		outputPath := ""
		if export && app.Cfg.Run.DefaultOutputDir != "" {
			outputPath = filepath.Join(app.Cfg.Run.DefaultOutputDir, tableName+"."+fileType)
		}

		rowCount, runErr := svc.Run(ctx, tmpFile.Name(), target, outputPath, fileType)
		os.Remove(tmpFile.Name())
		if runErr != nil {
			return fmt.Errorf("pipeline step %s failed: %w", sqlFile, runErr)
		}
		ui.Success(fmt.Sprintf("  %s: %d rows", target, rowCount))
	}

	return nil
}

// doPromote swaps current.* views to point at the specified version.
func doPromote(ctx context.Context, app *App) error {
	if runVersion <= 0 {
		return fmt.Errorf("--promote requires --version to be set (e.g., --version 1)")
	}

	silverDB := app.SilverDB()
	svc := services.NewSilverService(silverDB, app.Logger)

	ui.Info(fmt.Sprintf("Promoting silver version %d...", runVersion))
	if err := svc.Promote(ctx, runVersion); err != nil {
		return fmt.Errorf("promote failed: %w", err)
	}
	ui.Success(fmt.Sprintf("Promoted version %d to production", runVersion))
	return nil
}

// doSingleRun executes a single SQL file.
func doSingleRun(ctx context.Context, app *App) error {
	fileType := runFileType
	if fileType == "" {
		fileType = app.Cfg.Run.DefaultFileType
	}

	targetTable := runTargetTable
	if targetTable == "" {
		targetTable = deriveTargetFromFilename(runSQLFile)
	}

	svc := buildRunService(app, targetTable)
	rowCount, err := svc.Run(ctx, runSQLFile, targetTable, runOutputPath, fileType)
	if err != nil {
		return err
	}

	if runOutputPath != "" {
		fi, _ := os.Stat(runOutputPath)
		if fi != nil {
			ui.Success(fmt.Sprintf("Wrote %d rows to %s (%s)", rowCount, runOutputPath, humanizeBytes(fi.Size())))
		}
	}
	return nil
}

// buildRunService creates the appropriate RunService based on target table and app mode.
func buildRunService(app *App, targetTable string) *services.RunService {
	if app.Multi != nil {
		switch {
		case isFeatTarget(targetTable):
			return services.NewRunService(app.FeatureDB(), app.State, app.Logger)
		default:
			return services.NewRunService(app.SilverDB(), app.State, app.Logger)
		}
	}
	return services.NewRunService(app.WH, app.State, app.Logger)
}

func humanizeBytes(b int64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func init() {
	runCmd.Flags().StringVar(&runSQLFile, "sql-file", "", "path to SQL feature file")
	runCmd.Flags().StringVar(&runSQLDir, "sql-dir", "", "directory of SQL files to discover and run (sorted by numeric prefix)")
	runCmd.Flags().StringVar(&runTargetTable, "target-table", "", "target table (e.g., v1.order_enriched)")
	runCmd.Flags().StringVar(&runTargetSchema, "target-schema", "", "target schema for --sql-dir (default: derived from directory path)")
	runCmd.Flags().StringVar(&runOutputPath, "output", "", "output file path")
	runCmd.Flags().StringVar(&runFileType, "file-type", "", "output file type (parquet, csv)")
	runCmd.Flags().BoolVar(&runRefresh, "refresh", false, "snapshot raw.duckdb into silver.duckdb v0")
	runCmd.Flags().BoolVar(&runPipeline, "pipeline", false, "discover and run all SQL files in sql/silver/ and sql/feat/")
	runCmd.Flags().BoolVar(&runPromote, "promote", false, "swap current.* views to the specified --version")
	runCmd.Flags().IntVar(&runVersion, "version", 0, "silver version number (used with --promote)")
	rootCmd.AddCommand(runCmd)
}
