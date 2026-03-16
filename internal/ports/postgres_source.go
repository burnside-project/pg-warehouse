package ports

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// PostgresSource defines the contract for reading data from PostgreSQL.
type PostgresSource interface {
	// Ping checks connectivity to the PostgreSQL server.
	Ping(ctx context.Context) error

	// ListTables returns all table names in the given schema.
	ListTables(ctx context.Context, schema string) ([]string, error)

	// GetTableSchema returns column metadata for a table.
	GetTableSchema(ctx context.Context, table string) ([]models.ColumnInfo, error)

	// FetchFull reads all rows from a table for full snapshot sync.
	// Returns rows as a slice of maps.
	FetchFull(ctx context.Context, table string, batchSize int) ([]map[string]any, error)

	// FetchIncremental reads rows where watermarkColumn > lastWatermark.
	FetchIncremental(ctx context.Context, table string, watermarkColumn string, lastWatermark string, batchSize int) ([]map[string]any, error)

	// Close releases the PostgreSQL connection.
	Close() error
}
