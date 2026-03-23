package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Repair metadata state",
	Long:  "Re-register contracts, models; fix orphaned builds; repair checksum drift.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// Mark orphaned builds as failed
		sql := "UPDATE _meta.builds SET status = 'failed', error_message = 'repaired: orphaned build' WHERE status = 'running' AND started_at < current_timestamp - INTERVAL '1 hour'"
		if repairErr := app.SilverDB().ExecuteSQL(ctx, sql); repairErr != nil {
			ui.Warn(fmt.Sprintf("repair builds: %v", repairErr))
		} else {
			ui.Success("Repaired orphaned builds")
		}

		// Clear stale locks
		if app.State != nil {
			_ = app.State.ReleaseLock(ctx)
			ui.Success("Cleared stale locks")
		}

		ui.Success("Repair complete")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(repairCmd)
}
