package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "Show build and promotion history",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// Query builds
		builds, bErr := app.SilverDB().QueryRows(ctx,
			"SELECT build_id, release_name, release_version, status, model_count, duration_ms, started_at FROM _meta.builds ORDER BY build_id DESC LIMIT 20", 20)
		if bErr == nil && len(builds) > 0 {
			ui.Info("Recent Builds")
			headers := []string{"ID", "RELEASE", "VERSION", "STATUS", "MODELS", "DURATION", "STARTED"}
			var rows [][]string
			for _, b := range builds {
				rows = append(rows, []string{
					fmt.Sprintf("%v", b["build_id"]),
					fmt.Sprintf("%v", b["release_name"]),
					fmt.Sprintf("%v", b["release_version"]),
					fmt.Sprintf("%v", b["status"]),
					fmt.Sprintf("%v", b["model_count"]),
					fmt.Sprintf("%vms", b["duration_ms"]),
					fmt.Sprintf("%v", b["started_at"]),
				})
			}
			ui.Table(headers, rows)
		} else {
			ui.Info("No builds recorded yet.")
		}

		// Query promotions
		promos, pErr := app.SilverDB().QueryRows(ctx,
			"SELECT release_name, release_version, environment, build_id, promoted_at FROM _meta.promotions ORDER BY promoted_at DESC LIMIT 10", 10)
		if pErr == nil && len(promos) > 0 {
			fmt.Println()
			ui.Info("Recent Promotions")
			headers := []string{"RELEASE", "VERSION", "ENV", "BUILD", "PROMOTED"}
			var rows [][]string
			for _, p := range promos {
				rows = append(rows, []string{
					fmt.Sprintf("%v", p["release_name"]),
					fmt.Sprintf("%v", p["release_version"]),
					fmt.Sprintf("%v", p["environment"]),
					fmt.Sprintf("%v", p["build_id"]),
					fmt.Sprintf("%v", p["promoted_at"]),
				})
			}
			ui.Table(headers, rows)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(historyCmd)
}
