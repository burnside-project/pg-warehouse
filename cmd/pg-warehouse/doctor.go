package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/adapters/duckdb"
	"github.com/burnside-project/pg-warehouse/internal/adapters/fileconfig"
	"github.com/burnside-project/pg-warehouse/internal/adapters/postgres"
	"github.com/burnside-project/pg-warehouse/internal/config"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Validate configuration and connectivity",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		loader := fileconfig.NewLoader()
		cfg, err := loader.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}
		config.ApplyDefaults(cfg)

		logger := logging.NewLogger(cfg.Logging.Level, cfg.Logging.Format)

		wh := duckdb.NewWarehouse(cfg.DuckDB.Path)
		defer wh.Close()

		var pgSource *postgres.Source
		if cfg.Postgres.URL != "" {
			pgSource, err = postgres.NewSource(cfg.Postgres)
			if err != nil {
				ui.Warn(fmt.Sprintf("postgres: could not create source: %v", err))
			} else {
				defer pgSource.Close()
			}
		}

		svc := services.NewDoctorService(loader, pgSource, wh, logger)
		checks := svc.RunChecks(ctx, cfg)

		if ui.IsJSON() {
			return ui.JSON(checks)
		}

		for _, check := range checks {
			msg := fmt.Sprintf("%s: %s", check.Name, check.Detail)
			switch check.Status {
			case "ok":
				ui.Success(msg)
			case "warn":
				ui.Warn(msg)
			case "fail":
				ui.Error(msg)
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(doctorCmd)
}
