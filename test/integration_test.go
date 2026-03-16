//go:build integration

package test

import (
	"context"
	"testing"

	"github.com/burnside-project/pg-warehouse/internal/adapters/duckdb"
	"github.com/burnside-project/pg-warehouse/internal/models"
)

// TestIntegration_Bootstrap tests that the DuckDB warehouse bootstraps correctly.
// Run with: go test -tags=integration ./test/
func TestIntegration_Bootstrap(t *testing.T) {
	ctx := context.Background()

	wh := duckdb.NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open warehouse: %v", err)
	}
	defer wh.Close()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap warehouse: %v", err)
	}

	tables := []string{
		"meta.sync_state",
		"meta.sync_history",
		"meta.feature_runs",
		"meta.feature_dependencies",
	}

	for _, table := range tables {
		exists, err := wh.TableExists(ctx, table)
		if err != nil {
			t.Errorf("failed to check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %s to exist after bootstrap", table)
		}
	}
}

// TestIntegration_TypedSyncAndExport tests bootstrap, typed insert, feature SQL, and export.
func TestIntegration_TypedSyncAndExport(t *testing.T) {
	ctx := context.Background()

	wh := duckdb.NewWarehouse("")
	if err := wh.Open(ctx); err != nil {
		t.Fatalf("failed to open warehouse: %v", err)
	}
	defer wh.Close()

	if err := wh.Bootstrap(ctx); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	columns := []models.ColumnInfo{
		{Name: "id", Type: "integer", Position: 1},
		{Name: "name", Type: "text", Position: 2},
		{Name: "amount", Type: "numeric", Position: 3},
		{Name: "active", Type: "boolean", Position: 4},
	}

	rows := []map[string]any{
		{"id": 1, "name": "Alice", "amount": 100.50, "active": true},
		{"id": 2, "name": "Bob", "amount": 200.75, "active": false},
		{"id": 3, "name": "Charlie", "amount": 50.00, "active": true},
	}

	if err := wh.CreateTableFromRows(ctx, "raw.customers", rows, columns); err != nil {
		t.Fatalf("failed to create typed table: %v", err)
	}

	count, err := wh.CountRows(ctx, "raw.customers")
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}

	// Run a feature SQL
	featureSQL := `CREATE OR REPLACE TABLE feat.customer_summary AS
		SELECT COUNT(*) AS total_customers, SUM(amount) AS total_amount FROM raw.customers WHERE active = true`
	if err := wh.ExecuteSQL(ctx, featureSQL); err != nil {
		t.Fatalf("failed to run feature SQL: %v", err)
	}

	exists, err := wh.TableExists(ctx, "feat.customer_summary")
	if err != nil {
		t.Fatalf("failed to check feat table: %v", err)
	}
	if !exists {
		t.Fatal("expected feat.customer_summary to exist")
	}

	// Export to CSV
	tmpFile := t.TempDir() + "/summary.csv"
	if err := wh.ExportTable(ctx, "feat.customer_summary", tmpFile, "csv"); err != nil {
		t.Fatalf("failed to export: %v", err)
	}
}

// TestIntegration_FullSyncWorkflow is a scaffold for testing with a real PostgreSQL instance.
// TODO: Implement with test PostgreSQL instance and seed data.
func TestIntegration_FullSyncWorkflow(t *testing.T) {
	t.Skip("TODO: implement with test PostgreSQL instance")
}
