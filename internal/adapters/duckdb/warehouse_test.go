package duckdb

import (
	"context"
	"testing"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

func TestPgTypeToDuckDB(t *testing.T) {
	tests := []struct {
		pgType   string
		expected string
	}{
		{"integer", "INTEGER"},
		{"int4", "INTEGER"},
		{"bigint", "BIGINT"},
		{"smallint", "SMALLINT"},
		{"boolean", "BOOLEAN"},
		{"numeric", "DOUBLE"},
		{"double precision", "DOUBLE"},
		{"text", "VARCHAR"},
		{"character varying", "VARCHAR"},
		{"timestamp without time zone", "TIMESTAMP"},
		{"timestamptz", "TIMESTAMPTZ"},
		{"date", "DATE"},
		{"uuid", "UUID"},
		{"jsonb", "JSON"},
		{"bytea", "BLOB"},
		{"unknown_type", "VARCHAR"},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			got := pgTypeToDuckDB(tt.pgType)
			if got != tt.expected {
				t.Errorf("pgTypeToDuckDB(%q) = %q, want %q", tt.pgType, got, tt.expected)
			}
		})
	}
}

func TestWarehouse_BootstrapCreatesSchemas(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open in-memory duckdb: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	// Verify schemas exist by creating and checking a table in each
	schemas := []string{"raw", "stage", "feat"}
	for _, schema := range schemas {
		table := schema + ".bootstrap_test"
		if _, err := wh.db.ExecContext(ctx, "CREATE TABLE "+table+" (id INTEGER)"); err != nil {
			t.Errorf("schema %s not created: %v", schema, err)
		}
		exists, err := wh.TableExists(ctx, table)
		if err != nil {
			t.Errorf("failed to check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %s to exist", table)
		}
	}
}

func TestWarehouse_CreateTableFromRowsWithTypes(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	columns := []models.ColumnInfo{
		{Name: "id", Type: "integer", Position: 1},
		{Name: "name", Type: "text", Position: 2},
		{Name: "active", Type: "boolean", Position: 3},
	}

	rows := []map[string]any{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
	}

	if err := wh.CreateTableFromRows(ctx, "raw.test_table", rows, columns); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	exists, err := wh.TableExists(ctx, "raw.test_table")
	if err != nil {
		t.Fatalf("failed to check table: %v", err)
	}
	if !exists {
		t.Fatal("expected raw.test_table to exist")
	}

	count, err := wh.CountRows(ctx, "raw.test_table")
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestWarehouse_CreateTableFromRowsWithoutTypes(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	rows := []map[string]any{
		{"id": "1", "name": "Alice"},
	}

	// nil columns should fall back to VARCHAR
	if err := wh.CreateTableFromRows(ctx, "raw.fallback_table", rows, nil); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	count, err := wh.CountRows(ctx, "raw.fallback_table")
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestWarehouse_QueryRowsRejectsNonSelect(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	_, err := wh.QueryRows(ctx, "CREATE TABLE foo (id INT)", 10)
	if err == nil {
		t.Error("expected error for non-SELECT query")
	}
}

func TestWarehouse_QueryRowsAcceptsSelect(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	rows, err := wh.QueryRows(ctx, "SELECT 1 AS num, 'hello' AS msg", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestWarehouse_ExportTable(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	rows := []map[string]any{
		{"id": 1, "name": "test"},
	}
	columns := []models.ColumnInfo{
		{Name: "id", Type: "integer"},
		{Name: "name", Type: "text"},
	}
	if err := wh.CreateTableFromRows(ctx, "raw.export_test", rows, columns); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tmpFile := t.TempDir() + "/test.csv"
	if err := wh.ExportTable(ctx, "raw.export_test", tmpFile, "csv"); err != nil {
		t.Fatalf("failed to export: %v", err)
	}

	tmpParquet := t.TempDir() + "/test.parquet"
	if err := wh.ExportTable(ctx, "raw.export_test", tmpParquet, "parquet"); err != nil {
		t.Fatalf("failed to export parquet: %v", err)
	}
}

func TestWarehouse_BatchInsert(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	// Create 2500 rows to test batch boundary (batchSize=1000)
	var rows []map[string]any
	for i := 0; i < 2500; i++ {
		rows = append(rows, map[string]any{"id": i, "val": "test"})
	}
	columns := []models.ColumnInfo{
		{Name: "id", Type: "integer"},
		{Name: "val", Type: "text"},
	}

	if err := wh.CreateTableFromRows(ctx, "raw.batch_test", rows, columns); err != nil {
		t.Fatalf("failed to create table with batched inserts: %v", err)
	}

	count, err := wh.CountRows(ctx, "raw.batch_test")
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 2500 {
		t.Errorf("expected 2500 rows, got %d", count)
	}
}
