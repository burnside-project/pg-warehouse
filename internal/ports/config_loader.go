package ports

import "github.com/burnside-project/pg-warehouse/internal/models"

// ConfigLoader defines the contract for loading project configuration.
type ConfigLoader interface {
	// Load reads and parses the configuration from the given path.
	Load(path string) (*models.ProjectConfig, error)

	// Validate checks the loaded configuration for required fields.
	Validate(cfg *models.ProjectConfig) error
}
