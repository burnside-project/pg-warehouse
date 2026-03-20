package models

// ProjectConfig holds the top-level project configuration.
type ProjectConfig struct {
	Project  ProjectInfo `yaml:"project"`
	Postgres PostgresCfg `yaml:"postgres"`
	DuckDB   DuckDBCfg   `yaml:"duckdb"`
	State    StateCfg    `yaml:"state"`
	CDC      CDCCfg      `yaml:"cdc"`
	Sync     SyncCfg     `yaml:"sync"`
	Run      RunCfg      `yaml:"run"`
	Logging  LoggingCfg  `yaml:"logging"`
}

// ProjectInfo holds the project name.
type ProjectInfo struct {
	Name string `yaml:"name"`
}

// PostgresCfg holds PostgreSQL connection settings.
type PostgresCfg struct {
	URL            string `yaml:"url"`
	Schema         string `yaml:"schema"`
	MaxConns       int    `yaml:"max_conns"`
	ConnectTimeout string `yaml:"connect_timeout"`
	QueryTimeout   string `yaml:"query_timeout"`
}

// StateCfg holds SQLite state DB settings.
type StateCfg struct {
	Path string `yaml:"path"`
}

// CDCCfg holds CDC replication settings.
type CDCCfg struct {
	Enabled          bool     `yaml:"enabled"`
	PublicationName  string   `yaml:"publication_name"`
	SlotName         string   `yaml:"slot_name"`
	Tables           []string `yaml:"tables"`
	EpochIntervalSec int      `yaml:"epoch_interval_sec"`
	EpochMaxRows     int      `yaml:"epoch_max_rows"`

	// Guardrails: prevent disk fill on PostgreSQL
	MaxLagBytes    int64 `yaml:"max_lag_bytes"`     // Stop CDC if replication lag exceeds this (bytes). 0 = disabled.
	DropSlotOnExit bool  `yaml:"drop_slot_on_exit"` // Drop replication slot on graceful/crash exit to prevent orphaned WAL accumulation.
	HealthCheckSec int   `yaml:"health_check_sec"`  // Interval for lag health checks (seconds). 0 = use confirm ticker (10s).
}

// DuckDBCfg holds DuckDB settings.
type DuckDBCfg struct {
	Path    string `yaml:"path"`    // Legacy single-file mode
	Raw     string `yaml:"raw"`     // Multi-file: CDC black box (raw.duckdb)
	Silver  string `yaml:"silver"`  // Multi-file: development platform (silver.duckdb)
	Feature string `yaml:"feature"` // Multi-file: analytics output (feature.duckdb)
}

// IsMultiFileMode returns true when the raw field is set,
// indicating that three separate DuckDB files should be used.
func (d DuckDBCfg) IsMultiFileMode() bool {
	return d.Raw != ""
}

// SyncCfg holds sync configuration.
type SyncCfg struct {
	Mode             string        `yaml:"mode"`
	DefaultBatchSize int           `yaml:"default_batch_size"`
	Tables           []TableConfig `yaml:"tables"`
}

// TableConfig holds per-table sync configuration.
type TableConfig struct {
	Name            string   `yaml:"name"`
	TargetSchema    string   `yaml:"target_schema"`
	PrimaryKey      []string `yaml:"primary_key"`
	WatermarkColumn string   `yaml:"watermark_column"`
}

// RunCfg holds run-related configuration.
type RunCfg struct {
	DefaultOutputDir string `yaml:"default_output_dir"`
	DefaultFileType  string `yaml:"default_file_type"`
}

// LoggingCfg holds logging configuration.
type LoggingCfg struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}
