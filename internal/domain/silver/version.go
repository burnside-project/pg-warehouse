package silver

import (
	"fmt"
	"strings"
)

const (
	SchemaPrefix  = "v"
	SchemaCurrent = "current"
	SchemaMeta    = "_meta"
)

// VersionSchemaName returns the schema name for a given version number.
func VersionSchemaName(version int) string {
	return fmt.Sprintf("%s%d", SchemaPrefix, version)
}

// ValidateVersionedTarget checks that a target table name is valid for silver versioning.
// Accepts: "v1.table", "v2.table", "current.table"
func ValidateVersionedTarget(target string) error {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("target must be schema.table format, got: %s", target)
	}
	schema := parts[0]
	if schema == SchemaCurrent {
		return nil
	}
	if strings.HasPrefix(schema, SchemaPrefix) {
		// Check it's v followed by a number
		numPart := schema[len(SchemaPrefix):]
		for _, c := range numPart {
			if c < '0' || c > '9' {
				return fmt.Errorf("invalid version schema: %s (expected v<number>)", schema)
			}
		}
		if len(numPart) == 0 {
			return fmt.Errorf("invalid version schema: %s (missing version number)", schema)
		}
		return nil
	}
	return fmt.Errorf("silver target must use versioned schema (v1.table, v2.table, current.table), got: %s", schema)
}

// IsVersionedSchema checks if a schema name is a version schema (v1, v2, etc.)
func IsVersionedSchema(schema string) bool {
	if !strings.HasPrefix(schema, SchemaPrefix) {
		return false
	}
	numPart := schema[len(SchemaPrefix):]
	if len(numPart) == 0 {
		return false
	}
	for _, c := range numPart {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
