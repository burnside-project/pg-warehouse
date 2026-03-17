package main

import (
	"context"
	"fmt"
	"os"

	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var (
	runSQLFile     string
	runTargetTable string
	runOutputPath  string
	runFileType    string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run SQL feature job",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		fileType := runFileType
		if fileType == "" {
			fileType = app.Cfg.Run.DefaultFileType
		}

		svc := services.NewRunService(app.WH, app.State, app.Logger)
		rowCount, err := svc.Run(ctx, runSQLFile, runTargetTable, runOutputPath, fileType)
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
	},
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
	runCmd.Flags().StringVar(&runTargetTable, "target-table", "", "target table (must be feat.*)")
	runCmd.Flags().StringVar(&runOutputPath, "output", "", "output file path")
	runCmd.Flags().StringVar(&runFileType, "file-type", "", "output file type (parquet, csv)")
	_ = runCmd.MarkFlagRequired("sql-file")
	_ = runCmd.MarkFlagRequired("target-table")
	rootCmd.AddCommand(runCmd)
}
