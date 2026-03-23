package config

import (
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// Validate checks that the configuration has all required fields.
func Validate(cfg *models.ProjectConfig) error {
	// Release-only mode: no postgres required, skip CDC/sync validation
	hasPostgres := cfg.Postgres.URL != ""
	hasSyncTables := len(cfg.Sync.Tables) > 0

	// If sync tables are configured, postgres URL is required
	if hasSyncTables && !hasPostgres {
		return fmt.Errorf("postgres.url is required when sync tables are configured")
	}

	// If postgres is configured, require DuckDB and sync tables
	if hasPostgres {
		if cfg.DuckDB.IsMultiFileMode() {
			if cfg.DuckDB.Raw == "" {
				return fmt.Errorf("duckdb.raw is required in multi-file mode")
			}
			if cfg.DuckDB.Silver == "" {
				return fmt.Errorf("duckdb.silver is required in multi-file mode")
			}
			if cfg.DuckDB.Feature == "" {
				return fmt.Errorf("duckdb.feature is required in multi-file mode")
			}
		} else if cfg.DuckDB.Path == "" {
			return fmt.Errorf("duckdb.path is required")
		}
		if !hasSyncTables {
			return fmt.Errorf("at least one sync table must be configured")
		}
		for i, t := range cfg.Sync.Tables {
			if t.Name == "" {
				return fmt.Errorf("sync.tables[%d].name is required", i)
			}
			if len(t.PrimaryKey) == 0 {
				return fmt.Errorf("sync.tables[%d].primary_key is required", i)
			}
		}
	}
	return nil
}
