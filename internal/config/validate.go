package config

import (
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// Validate checks that the configuration has all required fields.
func Validate(cfg *models.ProjectConfig) error {
	if cfg.Postgres.URL == "" {
		return fmt.Errorf("postgres.url is required")
	}
	if cfg.DuckDB.IsMultiFileMode() {
		// Multi-file mode: validate all three paths are present.
		if cfg.DuckDB.Warehouse == "" {
			return fmt.Errorf("duckdb.warehouse is required in multi-file mode")
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
	if len(cfg.Sync.Tables) == 0 {
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
	return nil
}
