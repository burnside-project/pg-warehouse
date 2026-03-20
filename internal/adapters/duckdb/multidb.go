package duckdb

import (
	"context"
	"fmt"
)

// MultiDB coordinates three separate DuckDB warehouse instances.
// Each file has a single writer, enabling zero-downtime CDC.
type MultiDB struct {
	warehouse *Warehouse // CDC-owned, exclusive writer
	silver    *Warehouse // Silver development platform
	feature   *Warehouse // Feature analytics output
}

// NewMultiDB creates a new MultiDB coordinator from three DuckDB file paths.
func NewMultiDB(warehousePath, silverPath, featurePath string) *MultiDB {
	return &MultiDB{
		warehouse: &Warehouse{path: warehousePath},
		silver:    &Warehouse{path: silverPath},
		feature:   &Warehouse{path: featurePath},
	}
}

// OpenAll opens all three DuckDB files.
func (m *MultiDB) OpenAll(ctx context.Context) error {
	if err := m.warehouse.Open(ctx); err != nil {
		return fmt.Errorf("open warehouse: %w", err)
	}
	if err := m.silver.Open(ctx); err != nil {
		return fmt.Errorf("open silver: %w", err)
	}
	if err := m.feature.Open(ctx); err != nil {
		return fmt.Errorf("open feature: %w", err)
	}
	return nil
}

// CloseAll closes all three DuckDB files, returning the first error encountered.
func (m *MultiDB) CloseAll() error {
	var firstErr error
	if err := m.feature.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := m.silver.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := m.warehouse.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Warehouse returns the CDC-owned warehouse instance.
func (m *MultiDB) Warehouse() *Warehouse { return m.warehouse }

// Silver returns the silver development platform instance.
func (m *MultiDB) Silver() *Warehouse { return m.silver }

// Feature returns the feature analytics output instance.
func (m *MultiDB) Feature() *Warehouse { return m.feature }
