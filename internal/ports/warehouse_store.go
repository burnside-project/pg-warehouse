package ports

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// WarehouseStore defines the contract for the local DuckDB warehouse.
type WarehouseStore interface {
	// Open establishes a connection to the warehouse database.
	Open(ctx context.Context) error

	// Close releases the warehouse connection.
	Close() error

	// Bootstrap creates schemas and metadata tables.
	Bootstrap(ctx context.Context) error

	// ExecuteSQL runs arbitrary SQL against the warehouse.
	ExecuteSQL(ctx context.Context, sql string) error

	// ExecuteSQLWithArgs runs parameterized SQL against the warehouse.
	ExecuteSQLWithArgs(ctx context.Context, sql string, args ...any) error

	// TableExists checks whether a table exists in the warehouse.
	TableExists(ctx context.Context, table string) (bool, error)

	// CountRows returns the row count for a table.
	CountRows(ctx context.Context, table string) (int64, error)

	// CreateTableFromRows creates a table and inserts rows from a source fetch.
	// If columns is provided, proper types are used; otherwise falls back to VARCHAR.
	CreateTableFromRows(ctx context.Context, table string, rows []map[string]any, columns []models.ColumnInfo) error

	// InsertRows inserts rows into an existing table.
	InsertRows(ctx context.Context, table string, rows []map[string]any) error

	// MergeStageToRaw merges staged data into the raw table using primary keys.
	MergeStageToRaw(ctx context.Context, stageTable string, rawTable string, primaryKeys []string) error

	// ExportTable exports a warehouse table to a file.
	ExportTable(ctx context.Context, table string, path string, fileType string) error

	// QueryRows executes a SELECT query and returns rows for preview.
	QueryRows(ctx context.Context, query string, limit int) ([]map[string]any, error)
}
