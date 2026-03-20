package duckdb

import (
	"context"
	"fmt"
)

// AttachReadOnly attaches another DuckDB file as a read-only database.
func (w *Warehouse) AttachReadOnly(ctx context.Context, path string, alias string) error {
	sql := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", path, quoteIdentifier(alias))
	_, err := w.db.ExecContext(ctx, sql)
	return err
}

// DetachDatabase detaches a previously attached database.
func (w *Warehouse) DetachDatabase(ctx context.Context, alias string) error {
	sql := fmt.Sprintf("DETACH %s", quoteIdentifier(alias))
	_, err := w.db.ExecContext(ctx, sql)
	return err
}
