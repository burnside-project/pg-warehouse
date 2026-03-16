package main

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/services"
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
		return svc.Run(ctx, runSQLFile, runTargetTable, runOutputPath, fileType)
	},
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
