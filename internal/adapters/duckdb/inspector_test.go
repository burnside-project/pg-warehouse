package duckdb

import (
	"context"
	"testing"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

func TestInspector_ListTables(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	// Create a test table in raw schema
	columns := []models.ColumnInfo{{Name: "id", Type: "integer"}}
	rows := []map[string]any{{"id": 1}}
	if err := wh.CreateTableFromRows(ctx, "raw.test_inspect", rows, columns); err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	inspector := NewInspector(wh.DB())
	tables, err := inspector.ListTables(ctx)
	if err != nil {
		t.Fatalf("failed to list tables: %v", err)
	}

	if len(tables) < 1 {
		t.Error("expected at least 1 table")
	}

	found := false
	for _, table := range tables {
		if table.Schema == "raw" && table.Name == "test_inspect" {
			found = true
			if table.RowCount != 1 {
				t.Errorf("expected 1 row, got %d", table.RowCount)
			}
		}
	}
	if !found {
		t.Error("expected to find raw.test_inspect in table list")
	}
}

func TestInspector_DescribeTable(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	// Create a test table to describe
	columns := []models.ColumnInfo{
		{Name: "id", Type: "integer"},
		{Name: "name", Type: "text"},
	}
	rows := []map[string]any{{"id": 1, "name": "test"}}
	if err := wh.CreateTableFromRows(ctx, "raw.describe_test", rows, columns); err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	inspector := NewInspector(wh.DB())
	schema, err := inspector.DescribeTable(ctx, "raw.describe_test")
	if err != nil {
		t.Fatalf("failed to describe table: %v", err)
	}

	if schema.Schema != "raw" || schema.Name != "describe_test" {
		t.Errorf("unexpected schema: %s.%s", schema.Schema, schema.Name)
	}

	if len(schema.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(schema.Columns))
	}
}

func TestInspector_DescribeTable_InvalidName(t *testing.T) {
	ctx := context.Background()
	wh := NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer func() { _ = wh.Close() }()

	inspector := NewInspector(wh.DB())
	_, err := inspector.DescribeTable(ctx, "no_schema")
	if err == nil {
		t.Error("expected error for non-schema-qualified table name")
	}
}
