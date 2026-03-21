package duckdb

import (
	"context"
	"fmt"
)

// MultiDB coordinates the DuckDB instances for multi-file mode.
// raw.duckdb is CDC-owned (never opened here — only CDC opens it).
// silver.duckdb and feature.duckdb are pipeline-owned.
type MultiDB struct {
	rawPath string     // path to raw.duckdb (for snapshot/refresh, never opened via DuckDB)
	silver  *Warehouse // Silver development platform
	feature *Warehouse // Feature analytics output
}

// NewMultiDB creates a new MultiDB coordinator.
// rawPath is stored but NOT opened — CDC owns raw.duckdb exclusively.
// Only silver and feature are opened via DuckDB connections.
func NewMultiDB(rawPath, silverPath, featurePath string) *MultiDB {
	return &MultiDB{
		rawPath: rawPath,
		silver:  &Warehouse{path: silverPath},
		feature: &Warehouse{path: featurePath},
	}
}

// OpenAll opens silver and feature DuckDB files.
// raw.duckdb is NOT opened — CDC holds its lock.
func (m *MultiDB) OpenAll(ctx context.Context) error {
	if err := m.silver.Open(ctx); err != nil {
		return fmt.Errorf("open silver: %w", err)
	}
	if err := m.feature.Open(ctx); err != nil {
		return fmt.Errorf("open feature: %w", err)
	}
	return nil
}

// CloseAll closes silver and feature DuckDB files.
func (m *MultiDB) CloseAll() error {
	var firstErr error
	if err := m.feature.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := m.silver.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// RawPath returns the path to raw.duckdb (for snapshot/refresh).
// This file is never opened via DuckDB by the pipeline — only by CDC.
func (m *MultiDB) RawPath() string { return m.rawPath }

// Warehouse returns the raw DB instance for CDC use.
// In pipeline context, use RawPath() + snapshot instead.
// This opens the raw DB on demand for CDC commands only.
func (m *MultiDB) Warehouse() *Warehouse {
	return &Warehouse{path: m.rawPath}
}

// Silver returns the silver development platform instance.
func (m *MultiDB) Silver() *Warehouse { return m.silver }

// Feature returns the feature analytics output instance.
func (m *MultiDB) Feature() *Warehouse { return m.feature }
