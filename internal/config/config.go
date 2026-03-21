package config

import (
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// DefaultBatchSize is the default number of rows per sync batch.
const DefaultBatchSize = 50000

// DefaultOutputDir is the default output directory for exports.
const DefaultOutputDir = "./out"

// DefaultFileType is the default export file type.
const DefaultFileType = "parquet"

// DefaultLogLevel is the default logging level.
const DefaultLogLevel = "info"

// DefaultConfigFile is the default config file name.
const DefaultConfigFile = "pg-warehouse.yml"

// Default state, CDC, and postgres pool settings.
const (
	DefaultStatePath       = ".pgwh/state.db"
	DefaultMaxConns        = 2
	DefaultConnectTimeout  = "5s"
	DefaultQueryTimeout    = "30s"
	DefaultPublicationName = "pgwh_pub"
	DefaultSlotName        = "pgwh_slot"
)

// ApplyDefaults fills in missing configuration values with sensible defaults.
func ApplyDefaults(cfg *models.ProjectConfig) {
	if cfg.Sync.DefaultBatchSize == 0 {
		cfg.Sync.DefaultBatchSize = DefaultBatchSize
	}
	if cfg.Run.DefaultOutputDir == "" {
		cfg.Run.DefaultOutputDir = DefaultOutputDir
	}
	if cfg.Run.DefaultFileType == "" {
		cfg.Run.DefaultFileType = DefaultFileType
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = DefaultLogLevel
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "text"
	}
	if cfg.Postgres.Schema == "" {
		cfg.Postgres.Schema = "public"
	}
	if cfg.Postgres.MaxConns == 0 {
		cfg.Postgres.MaxConns = DefaultMaxConns
	}
	if cfg.Postgres.ConnectTimeout == "" {
		cfg.Postgres.ConnectTimeout = DefaultConnectTimeout
	}
	if cfg.Postgres.QueryTimeout == "" {
		cfg.Postgres.QueryTimeout = DefaultQueryTimeout
	}
	if cfg.State.Path == "" {
		cfg.State.Path = DefaultStatePath
	}
	if cfg.CDC.PublicationName == "" {
		cfg.CDC.PublicationName = DefaultPublicationName
	}
	if cfg.CDC.SlotName == "" {
		cfg.CDC.SlotName = DefaultSlotName
	}
	if cfg.CDC.EpochIntervalSec == 0 {
		cfg.CDC.EpochIntervalSec = 60
	}
	if cfg.CDC.EpochMaxRows == 0 {
		cfg.CDC.EpochMaxRows = 10000
	}
	if cfg.CDC.MaxLagBytes == 0 {
		cfg.CDC.MaxLagBytes = 5 * 1024 * 1024 * 1024 // 5GB default
	}
	if cfg.CDC.HealthCheckSec == 0 {
		cfg.CDC.HealthCheckSec = 60
	}

	// Cap max_conns at 5 per configuration contract (06-configuration.md)
	if cfg.Postgres.MaxConns > 5 {
		cfg.Postgres.MaxConns = 5
	}

	// Default target_schema to "raw" (06-configuration.md)
	for i := range cfg.Sync.Tables {
		if cfg.Sync.Tables[i].TargetSchema == "" {
			cfg.Sync.Tables[i].TargetSchema = "raw"
		}
	}

	// Multi-file DuckDB defaults: derive silver/feature paths from raw dir.
	if cfg.DuckDB.IsMultiFileMode() {
		dir := filepath.Dir(cfg.DuckDB.Raw)
		if cfg.DuckDB.Silver == "" {
			cfg.DuckDB.Silver = filepath.Join(dir, "silver.duckdb")
		}
		if cfg.DuckDB.Feature == "" {
			cfg.DuckDB.Feature = filepath.Join(dir, "feature.duckdb")
		}
	}
}
