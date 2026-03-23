package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/adapters/duckdb"
	"github.com/burnside-project/pg-warehouse/internal/adapters/fileconfig"
	"github.com/burnside-project/pg-warehouse/internal/adapters/postgres"
	"github.com/burnside-project/pg-warehouse/internal/adapters/sqlitestate"
	"github.com/burnside-project/pg-warehouse/internal/config"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/spf13/cobra"
)

var initPGURL string
var initDuckDBPath string

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize pg-warehouse project",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		// Init is special: config file may not exist yet, so we fall back to flags
		loader := fileconfig.NewLoader()
		cfg, err := loader.Load(cfgFile)
		if err != nil {
			cfg = &models.ProjectConfig{}
			cfg.Postgres.URL = initPGURL
			cfg.DuckDB.Path = initDuckDBPath
		}
		config.ApplyDefaults(cfg)

		if cfg.DuckDB.Path == "" {
			return fmt.Errorf("duckdb path is required (use --duckdb flag or config file)")
		}

		wh := duckdb.NewWarehouse(cfg.DuckDB.Path)

		var pgSource *postgres.Source
		if cfg.Postgres.URL != "" {
			pgSource, err = postgres.NewSource(cfg.Postgres)
			if err != nil {
				return fmt.Errorf("failed to connect to postgres: %w", err)
			}
			defer func() { _ = pgSource.Close() }()
		}

		svc := services.NewInitService(loader, wh, pgSource)
		if err := svc.Init(ctx, pgSource != nil); err != nil {
			return err
		}
		defer func() { _ = wh.Close() }()

		// Create state DB as documented (02-quickstart.md)
		stateStore, err := sqlitestate.NewStore(cfg.State.Path)
		if err != nil {
			return fmt.Errorf("failed to create state db: %w", err)
		}
		defer func() { _ = stateStore.Close() }()

		fmt.Println("pg-warehouse initialized successfully")
		fmt.Printf("  warehouse: %s\n", cfg.DuckDB.Path)
		fmt.Printf("  state:     %s\n", cfg.State.Path)

		// Scaffold project directories
		scaffoldDirs := []string{
			"models/silver",
			"models/features",
			"contracts",
			"releases",
		}
		for _, d := range scaffoldDirs {
			if mkErr := os.MkdirAll(d, 0o755); mkErr != nil {
				return fmt.Errorf("failed to create directory %s: %w", d, mkErr)
			}
		}

		// Create default release file
		defaultReleaseDir := filepath.Join("releases", "default")
		if mkErr := os.MkdirAll(defaultReleaseDir, 0o755); mkErr != nil {
			return fmt.Errorf("failed to create directory %s: %w", defaultReleaseDir, mkErr)
		}
		defaultReleasePath := filepath.Join(defaultReleaseDir, "0.1.0.yml")
		if _, statErr := os.Stat(defaultReleasePath); os.IsNotExist(statErr) {
			releaseContent := `release:
  name: default
  version: "0.1.0"
  description: Default release — builds all models
  models: []
  output:
    target: parquet
`
			if writeErr := os.WriteFile(defaultReleasePath, []byte(releaseContent), 0o644); writeErr != nil {
				return fmt.Errorf("failed to write %s: %w", defaultReleasePath, writeErr)
			}
		}

		fmt.Println()
		fmt.Println("Scaffolded:")
		fmt.Println("  models/silver/       — silver layer SQL models")
		fmt.Println("  models/features/     — feature layer SQL models")
		fmt.Println("  contracts/           — data contracts (YAML)")
		fmt.Println("  releases/            — release definitions (YAML)")

		return nil
	},
}

func init() {
	initCmd.Flags().StringVar(&initPGURL, "pg-url", "", "PostgreSQL connection URL")
	initCmd.Flags().StringVar(&initDuckDBPath, "duckdb", "./warehouse.duckdb", "DuckDB file path")
	rootCmd.AddCommand(initCmd)
}
