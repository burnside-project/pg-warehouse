package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

// Inspector implements ports.Inspector using DuckDB's information_schema.
type Inspector struct {
	db *sql.DB
}

// NewInspector creates a new DuckDB inspector adapter.
func NewInspector(db *sql.DB) *Inspector {
	return &Inspector{db: db}
}

// ListTables returns all tables in the warehouse grouped by schema.
func (i *Inspector) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	query := `SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema IN ('raw', 'stage', 'feat')
		ORDER BY table_schema, table_name`

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []models.TableInfo
	for rows.Next() {
		var t models.TableInfo
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("failed to scan table info: %w", err)
		}

		// Get row count for each table
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", t.Schema, t.Name)
		if err := i.db.QueryRowContext(ctx, countQuery).Scan(&t.RowCount); err != nil {
			t.RowCount = -1 // indicate error
		}

		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// DescribeTable returns the column schema for a given table.
func (i *Inspector) DescribeTable(ctx context.Context, table string) (*models.TableSchema, error) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("table name must be schema-qualified (e.g. raw.orders): %s", table)
	}
	schema, tableName := parts[0], parts[1]

	query := `SELECT column_name, data_type, is_nullable, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	rows, err := i.db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}
	defer rows.Close()

	result := &models.TableSchema{
		Schema: schema,
		Name:   tableName,
	}

	for rows.Next() {
		var col models.ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		col.Nullable = nullable == "YES"
		result.Columns = append(result.Columns, col)
	}

	if len(result.Columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", table)
	}

	return result, rows.Err()
}
