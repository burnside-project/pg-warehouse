package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

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
		defer pgSource.Close()

		svc := services.NewSyncService(pgSource, app.WH, app.State, app.Logger)

		totalTables := len(app.Cfg.Sync.Tables)
		progress := ui.NewProgress()
		progress.Start("syncing tables", totalTables)

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
		return nil
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
}
