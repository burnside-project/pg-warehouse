package warehouse

import "fmt"

// Schema constants for the DuckDB warehouse.
// State metadata is stored in SQLite, not DuckDB.
const (
	SchemaRaw   = "raw"
	SchemaStage = "stage"
	SchemaFeat  = "feat"
)

// AllSchemas returns the list of all warehouse schemas.
func AllSchemas() []string {
	return []string{SchemaRaw, SchemaStage, SchemaFeat}
}

// RawTableName returns the fully qualified raw table name.
func RawTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaRaw, tableName)
}

// StageTableName returns the fully qualified staging table name.
func StageTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaStage, tableName)
}

// FeatTableName returns the fully qualified feature table name.
func FeatTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaFeat, tableName)
}
