package ports

import (
	"context"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// Inspector defines the contract for inspecting the DuckDB warehouse.
// Sync state is managed by StateStore (SQLite), not the warehouse inspector.
type Inspector interface {
	// ListTables returns all tables in the warehouse grouped by schema.
	ListTables(ctx context.Context) ([]models.TableInfo, error)

	// DescribeTable returns the column schema for a given table.
	DescribeTable(ctx context.Context, table string) (*models.TableSchema, error)
}
