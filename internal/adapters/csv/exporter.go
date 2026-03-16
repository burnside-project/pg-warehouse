package csv

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// Exporter implements ports.Exporter for CSV output.
// It delegates to the WarehouseStore's ExportTable method.
type Exporter struct {
	warehouse ports.WarehouseStore
}

// NewExporter creates a new CSV exporter.
func NewExporter(warehouse ports.WarehouseStore) *Exporter {
	return &Exporter{warehouse: warehouse}
}

// Export writes the given table to a CSV file at the specified path.
func (e *Exporter) Export(ctx context.Context, table string, path string) error {
	if err := e.warehouse.ExportTable(ctx, table, path, "csv"); err != nil {
		return fmt.Errorf("csv export failed: %w", err)
	}
	return nil
}

// FileType returns "csv".
func (e *Exporter) FileType() string {
	return "csv"
}
