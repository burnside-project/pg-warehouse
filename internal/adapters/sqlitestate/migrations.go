package sqlitestate

import (
	"context"
	"database/sql"
	"fmt"
)

// migrate runs any pending schema migrations.
func migrate(ctx context.Context, db *sql.DB) error {
	// Get current version
	var version int
	err := db.QueryRowContext(ctx, "SELECT version FROM schema_version WHERE id = 1").Scan(&version)
	if err != nil {
		// Table may not exist yet or no row — start from 0
		version = 0
	}

	if version >= currentSchemaVersion {
		return nil // up to date
	}

	// Apply migrations sequentially
	for v := version + 1; v <= currentSchemaVersion; v++ {
		migrationSQL, ok := migrations[v]
		if !ok {
			continue // no migration for this version (initial bootstrap covers it)
		}
		if _, err := db.ExecContext(ctx, migrationSQL); err != nil {
			return fmt.Errorf("migration to version %d failed: %w", v, err)
		}
	}

	// Update schema version
	_, err = db.ExecContext(ctx,
		`INSERT INTO schema_version (id, version, updated_at) VALUES (1, ?, datetime('now'))
		 ON CONFLICT(id) DO UPDATE SET version = excluded.version, updated_at = datetime('now')`,
		currentSchemaVersion)
	if err != nil {
		return fmt.Errorf("failed to update schema version: %w", err)
	}

	return nil
}

// migrations holds SQL for each version upgrade.
// Version 1 is the initial schema (created by bootstrapSQL).
// Future migrations go here as: migrations[2] = "ALTER TABLE ..."
var migrations = map[int]string{
	// Version 1: initial schema — handled by bootstrapSQL, no migration needed
}
