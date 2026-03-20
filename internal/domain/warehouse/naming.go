package warehouse

import "fmt"

// Schema constants for the DuckDB warehouse.
// State metadata is stored in SQLite, not DuckDB.
const (
	SchemaRaw    = "raw"    // Bronze — mirrored source tables from CDC/sync
	SchemaStage  = "stage"  // Internal — temporary merge buffer for incremental sync
	SchemaSilver = "silver" // Silver — user-facing curated/transformed tables
	SchemaFeat   = "feat"   // Gold — analytics-ready feature pipeline outputs
)

// CDC epoch metadata column names injected into stage rows.
const (
	ColEpoch   = "_epoch"
	ColDeleted = "_deleted"
)

// AllSchemas returns the list of all warehouse schemas.
func AllSchemas() []string {
	return []string{SchemaRaw, SchemaStage, SchemaSilver, SchemaFeat}
}

// RawTableName returns the fully qualified raw table name.
func RawTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaRaw, tableName)
}

// StageTableName returns the fully qualified staging table name.
func StageTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaStage, tableName)
}

// SilverTableName returns the fully qualified silver table name.
func SilverTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaSilver, tableName)
}

// FeatTableName returns the fully qualified feature table name.
func FeatTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", SchemaFeat, tableName)
}
