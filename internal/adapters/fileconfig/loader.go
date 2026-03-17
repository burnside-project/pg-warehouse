package fileconfig

import (
	"fmt"
	"os"

	"github.com/burnside-project/pg-warehouse/internal/config"
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
// Delegates to the centralized config.Validate to avoid duplication.
func (l *Loader) Validate(cfg *models.ProjectConfig) error {
	return config.Validate(cfg)
}
