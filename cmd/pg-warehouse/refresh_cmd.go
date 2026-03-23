package main

import (
	"context"

	"github.com/spf13/cobra"
)

var refreshCmd = &cobra.Command{
	Use:   "refresh",
	Short: "Snapshot raw.duckdb into silver.duckdb v0",
	Long: `Refresh snapshots raw.duckdb into silver.duckdb v0, making CDC-ingested
data available for downstream transformations.

This is the first step in a typical workflow:
  1. pg-warehouse refresh          — snapshot raw into silver v0
  2. pg-warehouse build            — materialize models in DAG order
  3. pg-warehouse validate         — check contracts and data quality
  4. pg-warehouse promote          — swap current.* views to latest version`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()
		return doRefresh(ctx, app)
	},
}

func init() {
	rootCmd.AddCommand(refreshCmd)
}
