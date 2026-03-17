package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/adapters/duckdb"
	"github.com/burnside-project/pg-warehouse/internal/adapters/fileconfig"
	"github.com/burnside-project/pg-warehouse/internal/adapters/postgres"
	"github.com/burnside-project/pg-warehouse/internal/adapters/sqlitestate"
	"github.com/burnside-project/pg-warehouse/internal/config"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
)

// App holds the shared application components used by CLI commands.
type App struct {
	Cfg    *models.ProjectConfig
	Logger *logging.Logger
	WH     *duckdb.Warehouse
	State  *sqlitestate.Store
	Loader *fileconfig.Loader
}

// NewApp loads config, validates, opens the warehouse and state DB.
func NewApp(ctx context.Context, cfgPath string) (*App, error) {
	loader := fileconfig.NewLoader()
	cfg, err := loader.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	config.ApplyDefaults(cfg)

	if err := config.Validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return buildApp(ctx, cfg, loader)
}

// NewAppMinimal loads config and opens warehouse/state without requiring sync table config.
func NewAppMinimal(ctx context.Context, cfgPath string) (*App, error) {
	loader := fileconfig.NewLoader()
	cfg, err := loader.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	config.ApplyDefaults(cfg)

	if cfg.DuckDB.Path == "" {
		return nil, fmt.Errorf("duckdb.path is required")
	}

	return buildApp(ctx, cfg, loader)
}

func buildApp(ctx context.Context, cfg *models.ProjectConfig, loader *fileconfig.Loader) (*App, error) {
	logger := logging.NewLogger(cfg.Logging.Level, cfg.Logging.Format)

	wh := duckdb.NewWarehouse(cfg.DuckDB.Path)
	if err := wh.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open warehouse: %w", err)
	}

	state, err := sqlitestate.NewStore(cfg.State.Path)
	if err != nil {
		_ = wh.Close()
		return nil, fmt.Errorf("failed to open state db: %w", err)
	}

	return &App{
		Cfg:    cfg,
		Logger: logger,
		WH:     wh,
		State:  state,
		Loader: loader,
	}, nil
}

// NewPostgresSource creates a PostgreSQL source from the app config.
func (a *App) NewPostgresSource() (*postgres.Source, error) {
	if a.Cfg.Postgres.URL == "" {
		return nil, fmt.Errorf("postgres.url is required")
	}
	return postgres.NewSource(a.Cfg.Postgres)
}

// Close releases all resources held by the App.
func (a *App) Close() {
	if a.State != nil {
		_ = a.State.Close()
	}
	if a.WH != nil {
		_ = a.WH.Close()
	}
}
