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
	Multi  *duckdb.MultiDB
	State  *sqlitestate.Store
	Loader *fileconfig.Loader
}

// WarehouseDB returns the CDC warehouse. In multi-file mode it returns
// the dedicated warehouse instance; otherwise it returns the single WH.
func (a *App) WarehouseDB() *duckdb.Warehouse {
	if a.Multi != nil {
		return a.Multi.Warehouse()
	}
	return a.WH
}

// SilverDB returns the silver development platform warehouse.
// In single-file mode it returns the single WH (all schemas colocated).
func (a *App) SilverDB() *duckdb.Warehouse {
	if a.Multi != nil {
		return a.Multi.Silver()
	}
	return a.WH
}

// FeatureDB returns the feature analytics output warehouse.
// In single-file mode it returns the single WH (all schemas colocated).
func (a *App) FeatureDB() *duckdb.Warehouse {
	if a.Multi != nil {
		return a.Multi.Feature()
	}
	return a.WH
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

	if cfg.DuckDB.Path == "" && !cfg.DuckDB.IsMultiFileMode() {
		return nil, fmt.Errorf("duckdb.path is required")
	}

	return buildApp(ctx, cfg, loader)
}

func buildApp(ctx context.Context, cfg *models.ProjectConfig, loader *fileconfig.Loader) (*App, error) {
	logger := logging.NewLogger(cfg.Logging.Level, cfg.Logging.Format)

	app := &App{
		Cfg:    cfg,
		Logger: logger,
		Loader: loader,
	}

	if cfg.DuckDB.IsMultiFileMode() {
		multi := duckdb.NewMultiDB(cfg.DuckDB.Warehouse, cfg.DuckDB.Silver, cfg.DuckDB.Feature)
		if err := multi.OpenAll(ctx); err != nil {
			return nil, fmt.Errorf("failed to open multi-db: %w", err)
		}
		// Bootstrap each database with its own schema set.
		if err := multi.Warehouse().Bootstrap(ctx); err != nil {
			_ = multi.CloseAll()
			return nil, fmt.Errorf("failed to bootstrap warehouse db: %w", err)
		}
		if err := multi.Silver().BootstrapSilver(ctx); err != nil {
			_ = multi.CloseAll()
			return nil, fmt.Errorf("failed to bootstrap silver db: %w", err)
		}
		if err := multi.Feature().BootstrapFeature(ctx); err != nil {
			_ = multi.CloseAll()
			return nil, fmt.Errorf("failed to bootstrap feature db: %w", err)
		}
		app.Multi = multi
		// Set WH to the warehouse instance for backward compatibility.
		app.WH = multi.Warehouse()
	} else {
		wh := duckdb.NewWarehouse(cfg.DuckDB.Path)
		if err := wh.Open(ctx); err != nil {
			return nil, fmt.Errorf("failed to open warehouse: %w", err)
		}
		app.WH = wh
	}

	state, err := sqlitestate.NewStore(cfg.State.Path)
	if err != nil {
		app.Close()
		return nil, fmt.Errorf("failed to open state db: %w", err)
	}
	app.State = state

	return app, nil
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
	if a.Multi != nil {
		_ = a.Multi.CloseAll()
	} else if a.WH != nil {
		_ = a.WH.Close()
	}
}
