package parquet

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// Exporter implements ports.Exporter for Parquet output.
// It delegates to the WarehouseStore's ExportTable method.
type Exporter struct {
	warehouse ports.WarehouseStore
}

// NewExporter creates a new Parquet exporter.
func NewExporter(warehouse ports.WarehouseStore) *Exporter {
	return &Exporter{warehouse: warehouse}
}

// Export writes the given table to a Parquet file at the specified path.
func (e *Exporter) Export(ctx context.Context, table string, path string) error {
	if err := e.warehouse.ExportTable(ctx, table, path, "parquet"); err != nil {
		return fmt.Errorf("parquet export failed: %w", err)
	}
	return nil
}

// FileType returns "parquet".
func (e *Exporter) FileType() string {
	return "parquet"
}
