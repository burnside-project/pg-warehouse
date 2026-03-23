package main

import (
	"context"

	"github.com/spf13/cobra"
)

var refreshCmd = &cobra.Command{
	Use:   "refresh",
	Short: "Snapshot raw.duckdb into silver.duckdb v0",
	Long:  "Refresh runtime-managed curated layers from CDC inputs. This is platform-owned.",
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
