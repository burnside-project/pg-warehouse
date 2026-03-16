package fileconfig

import (
	"fmt"
	"os"

	"github.com/burnside-project/pg-warehouse/internal/models"
	"gopkg.in/yaml.v3"
)

// Loader implements ports.ConfigLoader by reading YAML files from disk.
type Loader struct{}

// NewLoader creates a new file-based config loader.
func NewLoader() *Loader {
	return &Loader{}
}

// Load reads and parses the YAML configuration from the given path.
func (l *Loader) Load(path string) (*models.ProjectConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg models.ProjectConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// Validate checks the loaded configuration for required fields.
func (l *Loader) Validate(cfg *models.ProjectConfig) error {
	if cfg.Postgres.URL == "" {
		return fmt.Errorf("postgres.url is required")
	}
	if cfg.DuckDB.Path == "" {
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
