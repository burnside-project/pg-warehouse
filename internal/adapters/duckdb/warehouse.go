package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/models"

	_ "github.com/marcboeker/go-duckdb"
)

const insertBatchSize = 1000

// quoteIdentifier safely quotes a possibly schema-qualified identifier for DuckDB.
func quoteIdentifier(name string) string {
	parts := strings.SplitN(name, ".", 2)
	for i, p := range parts {
		parts[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// Warehouse implements ports.WarehouseStore using DuckDB.
type Warehouse struct {
	db   *sql.DB
	path string
}

// NewWarehouse creates a new DuckDB warehouse adapter.
func NewWarehouse(path string) *Warehouse {
	return &Warehouse{path: path}
}

// DB returns the underlying *sql.DB handle for sharing with other adapters.
func (w *Warehouse) DB() *sql.DB {
	return w.db
}

// Open establishes a connection to DuckDB.
func (w *Warehouse) Open(ctx context.Context) error {
	db, err := sql.Open("duckdb", w.path)
	if err != nil {
		return fmt.Errorf("failed to open duckdb: %w", err)
	}
	w.db = db
	return nil
}

// Close releases the DuckDB connection.
func (w *Warehouse) Close() error {
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

// Bootstrap creates schemas and metadata tables.
func (w *Warehouse) Bootstrap(ctx context.Context) error {
	return w.ExecuteSQL(ctx, bootstrapSQL)
}

// ExecuteSQL runs arbitrary SQL against the warehouse.
func (w *Warehouse) ExecuteSQL(ctx context.Context, sqlStr string) error {
	statements := splitStatements(sqlStr)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute SQL: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}

// ExecuteSQLWithArgs runs a single parameterized SQL statement against the warehouse.
func (w *Warehouse) ExecuteSQLWithArgs(ctx context.Context, sqlStr string, args ...any) error {
	if _, err := w.db.ExecContext(ctx, sqlStr, args...); err != nil {
		return fmt.Errorf("failed to execute SQL: %w\nstatement: %s", err, sqlStr)
	}
	return nil
}

// TableExists checks whether a table exists in the warehouse.
func (w *Warehouse) TableExists(ctx context.Context, table string) (bool, error) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("table name must be schema-qualified: %s", table)
	}
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`
	var count int
	if err := w.db.QueryRowContext(ctx, query, parts[0], parts[1]).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}
	return count > 0, nil
}

// CountRows returns the row count for a table.
func (w *Warehouse) CountRows(ctx context.Context, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))
	var count int64
	if err := w.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// CreateTableFromRows creates a table and inserts rows.
// If columns is provided, proper DuckDB types are used; otherwise falls back to VARCHAR.
func (w *Warehouse) CreateTableFromRows(ctx context.Context, table string, rows []map[string]any, columns []models.ColumnInfo) error {
	if len(rows) == 0 {
		return nil
	}

	// Derive column names from first row (preserves order)
	var colNames []string
	for col := range rows[0] {
		colNames = append(colNames, col)
	}

	// Drop existing table
	quotedTable := quoteIdentifier(table)
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTable)
	if _, err := w.db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Build column type map from schema info
	typeMap := make(map[string]string)
	for _, col := range columns {
		typeMap[col.Name] = pgTypeToDuckDB(col.Type)
	}

	// Build CREATE TABLE with proper types
	var colDefs []string
	for _, col := range colNames {
		duckType, ok := typeMap[col]
		if !ok {
			duckType = "VARCHAR"
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdentifier(col), duckType))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", quotedTable, strings.Join(colDefs, ", "))
	if _, err := w.db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return w.InsertRows(ctx, table, rows)
}

// InsertRows inserts rows into an existing table using batched multi-row inserts.
func (w *Warehouse) InsertRows(ctx context.Context, table string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	var columns []string
	for col := range rows[0] {
		columns = append(columns, col)
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdentifier(col)
	}
	quotedTable := quoteIdentifier(table)

	for i := 0; i < len(rows); i += insertBatchSize {
		end := i + insertBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		var allValues []any
		var valueSets []string
		for _, row := range batch {
			placeholders := make([]string, len(columns))
			for j, col := range columns {
				allValues = append(allValues, row[col])
				placeholders[j] = fmt.Sprintf("$%d", len(allValues))
			}
			valueSets = append(valueSets, "("+strings.Join(placeholders, ", ")+")")
		}

		batchSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			quotedTable, strings.Join(quotedCols, ", "), strings.Join(valueSets, ", "))

		if _, err := w.db.ExecContext(ctx, batchSQL, allValues...); err != nil {
			return fmt.Errorf("failed to insert batch: %w", err)
		}
	}
	return nil
}

// MergeStageToRaw merges staged data into the raw table using primary keys.
// Dedup: if the stage table contains duplicate primary keys (e.g., a row updated
// multiple times between syncs), only the last row per PK is kept.
// The merge runs in a transaction to prevent data loss on crash.
func (w *Warehouse) MergeStageToRaw(ctx context.Context, stageTable string, rawTable string, primaryKeys []string) error {
	quotedRaw := quoteIdentifier(rawTable)
	quotedStage := quoteIdentifier(stageTable)
	quotedPKs := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		quotedPKs[i] = quoteIdentifier(pk)
	}
	pkList := strings.Join(quotedPKs, ", ")

	// Step 1: Dedup the stage table — keep only the last row per primary key.
	// Uses ROW_NUMBER() partitioned by PK, ordered by rowid descending (last inserted wins).
	dedupSQL := fmt.Sprintf(`
		DELETE FROM %s WHERE rowid NOT IN (
			SELECT MAX(rowid) FROM %s GROUP BY %s
		)`,
		quotedStage, quotedStage, pkList)

	// Step 2: Transactional merge — delete matching PKs from raw, insert from stage, drop stage.
	mergeSQL := fmt.Sprintf(`
		BEGIN TRANSACTION;
		DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s);
		INSERT INTO %s SELECT * FROM %s;
		DROP TABLE IF EXISTS %s;
		COMMIT;
	`, quotedRaw, pkList, pkList, quotedStage,
		quotedRaw, quotedStage,
		quotedStage)

	// Run dedup first (outside transaction — operates only on stage).
	if err := w.ExecuteSQL(ctx, dedupSQL); err != nil {
		return fmt.Errorf("failed to dedup stage table: %w", err)
	}

	return w.ExecuteSQL(ctx, mergeSQL)
}

// ExportTable exports a warehouse table to a file.
func (w *Warehouse) ExportTable(ctx context.Context, table string, path string, fileType string) error {
	quotedTable := quoteIdentifier(table)
	// Escape single quotes in file path to prevent injection
	safePath := strings.ReplaceAll(path, "'", "''")
	var exportSQL string
	switch strings.ToLower(fileType) {
	case "parquet":
		exportSQL = fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET)", quotedTable, safePath)
	case "csv":
		exportSQL = fmt.Sprintf("COPY %s TO '%s' (FORMAT CSV, HEADER)", quotedTable, safePath)
	default:
		return fmt.Errorf("unsupported export format: %s", fileType)
	}
	_, err := w.db.ExecContext(ctx, exportSQL)
	if err != nil {
		return fmt.Errorf("failed to export table: %w", err)
	}
	return nil
}

// QueryRows executes a SELECT query and returns rows for preview.
func (w *Warehouse) QueryRows(ctx context.Context, query string, limit int) ([]map[string]any, error) {
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return nil, fmt.Errorf("preview requires a SELECT or WITH query, got: %.30s", trimmed)
	}

	limitedQuery := fmt.Sprintf("SELECT * FROM (%s) AS preview LIMIT %d", trimmed, limit)
	rows, err := w.db.QueryContext(ctx, limitedQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute preview query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(columnNames))
		pointers := make([]any, len(columnNames))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		row := make(map[string]any, len(columnNames))
		for i, col := range columnNames {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// pgTypeToDuckDB maps PostgreSQL data types to DuckDB equivalents.
func pgTypeToDuckDB(pgType string) string {
	switch strings.ToLower(pgType) {
	case "integer", "int", "int4":
		return "INTEGER"
	case "bigint", "int8":
		return "BIGINT"
	case "smallint", "int2":
		return "SMALLINT"
	case "boolean", "bool":
		return "BOOLEAN"
	case "numeric", "decimal":
		return "DOUBLE"
	case "real", "float4":
		return "FLOAT"
	case "double precision", "float8":
		return "DOUBLE"
	case "text", "character varying", "varchar":
		return "VARCHAR"
	case "character", "char", "bpchar":
		return "VARCHAR"
	case "timestamp without time zone", "timestamp":
		return "TIMESTAMP"
	case "timestamp with time zone", "timestamptz":
		return "TIMESTAMPTZ"
	case "date":
		return "DATE"
	case "time without time zone", "time":
		return "TIME"
	case "uuid":
		return "UUID"
	case "jsonb", "json":
		return "JSON"
	case "bytea":
		return "BLOB"
	default:
		return "VARCHAR"
	}
}

// splitStatements splits a multi-statement SQL string by semicolons.
func splitStatements(sql string) []string {
	return strings.Split(sql, ";")
}
