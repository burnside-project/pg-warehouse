package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/spf13/cobra"
)

var (
	previewSQLFile string
	previewLimit   int
)

var previewCmd = &cobra.Command{
	Use:   "preview",
	Short: "Preview SQL feature output",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		svc := services.NewPreviewService(app.WH, app.Logger)
		rows, err := svc.Preview(ctx, previewSQLFile, previewLimit)
		if err != nil {
			return err
		}

		for i, row := range rows {
			fmt.Printf("Row %d: %v\n", i+1, row)
		}
		fmt.Printf("\n%d rows returned\n", len(rows))
		return nil
	},
}

func init() {
	previewCmd.Flags().StringVar(&previewSQLFile, "sql-file", "", "path to SQL feature file")
	previewCmd.Flags().IntVar(&previewLimit, "limit", 20, "max rows to preview")
	_ = previewCmd.MarkFlagRequired("sql-file")
	rootCmd.AddCommand(previewCmd)
}
