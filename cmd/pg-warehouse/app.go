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

// WarehouseDB returns the raw DuckDB for CDC commands.
// In multi-file mode, this creates a NEW connection to raw.duckdb (caller must open/close).
// In single-file mode, returns the shared WH.
func (a *App) WarehouseDB() *duckdb.Warehouse {
	if a.Multi != nil {
		return a.Multi.Warehouse()
	}
	return a.WH
}

// RawDB is an alias for WarehouseDB.
func (a *App) RawDB() *duckdb.Warehouse {
	return a.WarehouseDB()
}

// SilverDB returns the silver development platform warehouse.
func (a *App) SilverDB() *duckdb.Warehouse {
	if a.Multi != nil {
		return a.Multi.Silver()
	}
	return a.WH
}

// RawDB returns the raw CDC warehouse. In multi-file mode it returns
// the dedicated warehouse instance; otherwise it returns the single WH.
func (a *App) RawDB() *duckdb.Warehouse {
	if a.Multi != nil {
		return a.Multi.Warehouse() // Warehouse() in multidb.go returns the raw db
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
		multi := duckdb.NewMultiDB(cfg.DuckDB.Raw, cfg.DuckDB.Silver, cfg.DuckDB.Feature)
		if err := multi.OpenAll(ctx); err != nil {
			return nil, fmt.Errorf("failed to open multi-db: %w", err)
		}
		// Bootstrap silver and feature databases.
		// raw.duckdb is NOT opened here — CDC owns it exclusively.
		if err := multi.Silver().BootstrapSilver(ctx); err != nil {
			_ = multi.CloseAll()
			return nil, fmt.Errorf("failed to bootstrap silver db: %w", err)
		}
		if err := multi.Feature().BootstrapFeature(ctx); err != nil {
			_ = multi.CloseAll()
			return nil, fmt.Errorf("failed to bootstrap feature db: %w", err)
		}
		app.Multi = multi
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

// NewAppCDC loads config and opens ONLY raw.duckdb + state for CDC commands.
// In multi-file mode, silver.duckdb and feature.duckdb are NOT opened — CDC doesn't need them.
func NewAppCDC(ctx context.Context, cfgPath string) (*App, error) {
	loader := fileconfig.NewLoader()
	cfg, err := loader.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	config.ApplyDefaults(cfg)

	logger := logging.NewLogger(cfg.Logging.Level, cfg.Logging.Format)
	app := &App{
		Cfg:    cfg,
		Logger: logger,
		Loader: loader,
	}

	// Open raw.duckdb (or the single-file warehouse)
	rawPath := cfg.DuckDB.Path
	if cfg.DuckDB.IsMultiFileMode() {
		rawPath = cfg.DuckDB.Raw
	}
	if rawPath == "" {
		return nil, fmt.Errorf("duckdb path is required")
	}

	wh := duckdb.NewWarehouse(rawPath)
	if err := wh.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open raw.duckdb: %w", err)
	}
	if err := wh.Bootstrap(ctx); err != nil {
		_ = wh.Close()
		return nil, fmt.Errorf("failed to bootstrap raw.duckdb: %w", err)
	}
	app.WH = wh

	state, err := sqlitestate.NewStore(cfg.State.Path)
	if err != nil {
		_ = wh.Close()
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
