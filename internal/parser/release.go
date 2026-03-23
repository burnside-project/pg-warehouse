package parser

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/burnside-project/pg-warehouse/internal/domain/release"
)

// ReleaseFile is the YAML structure for release files.
type ReleaseFile struct {
	Release release.Release `yaml:"release"`
}

// ParseReleaseFile reads a YAML release file.
func ParseReleaseFile(path string) (*release.Release, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read release file: %w", err)
	}
	var f ReleaseFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse release YAML: %w", err)
	}
	f.Release.FilePath = path
	return &f.Release, nil
}
