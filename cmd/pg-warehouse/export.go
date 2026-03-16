package main

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/services"
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
		return svc.Export(ctx, exportTable, exportOutput, fileType)
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
