package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

// isFeatTarget returns true when the target table belongs to the feat schema.
func isFeatTarget(table string) bool {
	return strings.HasPrefix(strings.ToLower(table), "feat.")
}

// deriveTargetFromFilename extracts the target table from a SQL filename.
// sql/silver/v1/001_order_enriched.sql → v1.order_enriched
// sql/feat/001_sales_summary.sql → feat.sales_summary
func deriveTargetFromFilename(sqlFile string) string {
	base := filepath.Base(sqlFile)           // 001_order_enriched.sql
	name := strings.TrimSuffix(base, ".sql") // 001_order_enriched

	// Strip numeric prefix: 001_order_enriched → order_enriched
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
				name = name[i+1:]
				break
			}
		}
	}

	// Determine schema from directory path
	dir := filepath.Base(filepath.Dir(sqlFile)) // v1, feat, etc.
	return dir + "." + name
}

var (
	runSQLFile     string
	runTargetTable string
	runOutputPath  string
	runFileType    string
	runRefresh     bool
	runPipeline    bool
	runPromote     bool
	runVersion     int
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run SQL feature job",
	Long: `Run SQL transformations against the warehouse.

Modes:
  --refresh     Snapshot raw.duckdb into silver.duckdb v0 (refresh layer)
  --pipeline    Discover and run all SQL files in sql/silver/v1/ and sql/feat/
  --promote     Swap current.* views to point at the specified --version
  --sql-file    Run a single SQL file (legacy mode)

If none of --refresh, --pipeline, --promote, or --sql-file are provided, help is shown.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// If no mode flag is provided, show help.
		if !runRefresh && !runPipeline && !runPromote && runSQLFile == "" {
			return cmd.Help()
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

		// --promote: swap current.* views
		if runPromote {
			if err := doPromote(ctx, app); err != nil {
				return err
			}
		}

		// --sql-file: run a single file (legacy)
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

// doPipeline discovers SQL files in sql/silver/v1/ and sql/feat/, then runs each.
func doPipeline(ctx context.Context, app *App) error {
	fileType := runFileType
	if fileType == "" {
		fileType = app.Cfg.Run.DefaultFileType
	}

	// Phase 1: silver/v1 SQL files
	silverFiles, err := filepath.Glob("sql/silver/v1/*.sql")
	if err != nil {
		return fmt.Errorf("glob silver files: %w", err)
	}
	sort.Strings(silverFiles)

	for _, sqlFile := range silverFiles {
		target := deriveTargetFromFilename(sqlFile)
		ui.Info(fmt.Sprintf("Pipeline [silver]: %s -> %s", sqlFile, target))

		svc := buildRunService(app, target)
		rowCount, err := svc.Run(ctx, sqlFile, target, "", "")
		if err != nil {
			return fmt.Errorf("pipeline step %s failed: %w", sqlFile, err)
		}
		ui.Success(fmt.Sprintf("  %s: %d rows", target, rowCount))
	}

	// Phase 2: feat SQL files
	featFiles, err := filepath.Glob("sql/feat/*.sql")
	if err != nil {
		return fmt.Errorf("glob feat files: %w", err)
	}
	sort.Strings(featFiles)

	for _, sqlFile := range featFiles {
		target := deriveTargetFromFilename(sqlFile)
		ui.Info(fmt.Sprintf("Pipeline [feat]: %s -> %s", sqlFile, target))

		svc := buildRunService(app, target)

		// For feat targets, derive output path if a default output dir is configured.
		outputPath := ""
		if app.Cfg.Run.DefaultOutputDir != "" {
			base := strings.TrimSuffix(filepath.Base(sqlFile), ".sql")
			ft := fileType
			if ft == "" {
				ft = "parquet"
			}
			outputPath = filepath.Join(app.Cfg.Run.DefaultOutputDir, base+"."+ft)
		}

		rowCount, err := svc.Run(ctx, sqlFile, target, outputPath, fileType)
		if err != nil {
			return fmt.Errorf("pipeline step %s failed: %w", sqlFile, err)
		}
		ui.Success(fmt.Sprintf("  %s: %d rows", target, rowCount))
	}

	ui.Success("Pipeline complete")
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

// doSingleRun executes a single SQL file (legacy --sql-file mode).
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
			return services.NewRunServiceMulti(app.FeatureDB(), app.State, app.Logger,
				app.Cfg.DuckDB.Silver, "silver")
		default:
			return services.NewRunServiceMulti(app.SilverDB(), app.State, app.Logger,
				app.Cfg.DuckDB.Raw, "raw")
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
	runCmd.Flags().StringVar(&runTargetTable, "target-table", "", "target table (e.g., v1.order_enriched, feat.sales_summary)")
	runCmd.Flags().StringVar(&runOutputPath, "output", "", "output file path")
	runCmd.Flags().StringVar(&runFileType, "file-type", "", "output file type (parquet, csv)")
	runCmd.Flags().BoolVar(&runRefresh, "refresh", false, "snapshot raw.duckdb into silver.duckdb v0")
	runCmd.Flags().BoolVar(&runPipeline, "pipeline", false, "discover and run all SQL files in sql/silver/v1/ and sql/feat/")
	runCmd.Flags().BoolVar(&runPromote, "promote", false, "swap current.* views to the specified --version")
	runCmd.Flags().IntVar(&runVersion, "version", 0, "silver version number (used with --promote)")
	rootCmd.AddCommand(runCmd)
}
