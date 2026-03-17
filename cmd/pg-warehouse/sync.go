package main

import (
	"context"
	"fmt"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var syncMode string

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync PostgreSQL tables into DuckDB",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewApp(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		pgSource, err := app.NewPostgresSource()
		if err != nil {
			return err
		}
		defer func() { _ = pgSource.Close() }()

		svc := services.NewSyncService(pgSource, app.WH, app.State, app.Logger)

		// Apply mode override: CLI flag takes precedence over config
		mode := syncMode
		if mode == "" {
			mode = app.Cfg.Sync.Mode
		}
		if mode != "" {
			svc.SetModeOverride(mode)
		}

		totalTables := len(app.Cfg.Sync.Tables)
		progress := ui.NewProgress()
		progress.Start("syncing tables", totalTables)

		syncStart := time.Now()
		results, err := svc.SyncAll(ctx, app.Cfg.Sync.Tables, app.Cfg.Sync.DefaultBatchSize)
		if err != nil {
			return err
		}

		progress.Done()

		if ui.IsJSON() {
			return ui.JSON(results)
		}

		for _, r := range results {
			if r.Error != nil {
				ui.Error(fmt.Sprintf("%s: %v", r.TableName, r.Error))
			} else {
				ui.Success(fmt.Sprintf("%s: mode=%s rows=%d duration=%s", r.TableName, r.Mode, r.InsertedRows, r.Duration))
			}
		}
		fmt.Printf("Sync complete in %s\n", time.Since(syncStart).Round(100*time.Millisecond))
		return nil
	},
}

func init() {
	syncCmd.Flags().StringVar(&syncMode, "mode", "", "sync mode override (full, incremental)")
	rootCmd.AddCommand(syncCmd)
}
