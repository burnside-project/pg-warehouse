package feature

import (
	"fmt"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/domain/silver"
)

// reservedSchemas are schemas that user SQL must not write to.
var reservedSchemas = map[string]string{
	"raw":   "DANGER: raw.duckdb is owned exclusively by CDC. User SQL must not write to raw.*",
	"stage": "DANGER: stage.* is an internal CDC merge buffer. User SQL must not write to stage.*",
	"v0":    "DANGER: v0.* is the upstream data mirror populated by --refresh. User SQL must not write to v0.* — use v1, v2, etc.",
	"_meta": "DANGER: _meta.* is internal metadata managed by pg-warehouse. User SQL must not write to _meta.*",
}

// ValidateTargetSchema checks that a target schema is not reserved.
func ValidateTargetSchema(schema string) error {
	if msg, blocked := reservedSchemas[strings.ToLower(schema)]; blocked {
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// ValidateTargetTable ensures the target table is in the silver, feat, or versioned silver schema.
// Accepted prefixes: "silver.", "feat.", "current.", "v1.", "v2.", etc.
// Rejected: raw.*, stage.*, v0.*, _meta.*
func ValidateTargetTable(target string) error {
	if target == "" {
		return fmt.Errorf("target table must not be empty")
	}
	// Check reserved schemas
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		if err := ValidateTargetSchema(parts[0]); err != nil {
			return err
		}
	}
	// Allow classic prefixes
	if strings.HasPrefix(target, "feat.") || strings.HasPrefix(target, "silver.") {
		return nil
	}
	// Allow versioned silver targets (v1.table, v2.table, current.table)
	if err := silver.ValidateVersionedTarget(target); err == nil {
		return nil
	}
	return fmt.Errorf("target table must be in the 'silver', 'feat', or versioned silver schema (v1.table, current.table), got: %s", target)
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