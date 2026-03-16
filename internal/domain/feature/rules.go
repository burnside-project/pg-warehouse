package feature

import (
	"fmt"
	"strings"
)

// ValidateTargetTable ensures the target table is in the feat schema.
func ValidateTargetTable(target string) error {
	if target == "" {
		return fmt.Errorf("target table must not be empty")
	}
	if !strings.HasPrefix(target, "feat.") {
		return fmt.Errorf("target table must be in the 'feat' schema, got: %s", target)
	}
	return nil
}

// ValidateSQLFile checks that a SQL file path is non-empty and has a .sql extension.
func ValidateSQLFile(path string) error {
	if path == "" {
		return fmt.Errorf("SQL file path must not be empty")
	}
	if !strings.HasSuffix(strings.ToLower(path), ".sql") {
		return fmt.Errorf("SQL file must have .sql extension, got: %s", path)
	}
	return nil
}

// ValidateOutputType checks that the output type is supported.
func ValidateOutputType(fileType string) error {
	switch strings.ToLower(fileType) {
	case "parquet", "csv":
		return nil
	default:
		return fmt.Errorf("unsupported output type: %s (supported: parquet, csv)", fileType)
	}
}