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
	exportTable    string
	exportOutput   string
	exportFileType string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export warehouse table to file",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		fileType := exportFileType
		if fileType == "" {
			fileType = app.Cfg.Run.DefaultFileType
		}

		svc := services.NewExportService(app.WH, app.Logger)
		rowCount, err := svc.Export(ctx, exportTable, exportOutput, fileType)
		if err != nil {
			return err
		}

		fi, _ := os.Stat(exportOutput)
		if fi != nil {
			ui.Success(fmt.Sprintf("Exported %d rows to %s (%s)", rowCount, exportOutput, humanizeBytes(fi.Size())))
		}
		return nil
	},
}

func init() {
	exportCmd.Flags().StringVar(&exportTable, "table", "", "table to export (e.g. feat.customer_features)")
	exportCmd.Flags().StringVar(&exportOutput, "output", "", "output file path")
	exportCmd.Flags().StringVar(&exportFileType, "file-type", "", "output file type (parquet, csv)")
	_ = exportCmd.MarkFlagRequired("table")
	_ = exportCmd.MarkFlagRequired("output")
	rootCmd.AddCommand(exportCmd)
}
