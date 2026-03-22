package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// RefreshService populates v0.* in a target DuckDB by copying tables from an upstream DuckDB.
type RefreshService struct {
	target ports.WarehouseStore
	state  ports.StateStore
	logger *logging.Logger
}

// NewRefreshService creates a RefreshService.
func NewRefreshService(target ports.WarehouseStore, state ports.StateStore, logger *logging.Logger) *RefreshService {
	return &RefreshService{target: target, state: state, logger: logger}
}

// Refresh snapshots the source DuckDB file and copies tables from sourceSchema into v0.
// For silver: source=raw.duckdb, sourceSchema="raw"
// For feature: source=silver.duckdb, sourceSchema="current"
func (s *RefreshService) Refresh(ctx context.Context, sourcePath string, sourceSchema string) error {
	start := time.Now()

	// Step 1: Create snapshot (filesystem copy to avoid DuckDB lock conflicts)
	snapshotPath := filepath.Join(os.TempDir(), fmt.Sprintf("pgwh_snapshot_%d.duckdb", time.Now().UnixNano()))
	s.logger.Info("creating snapshot of %s", sourcePath)

	srcBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source db for snapshot: %w", err)
	}
	if err := os.WriteFile(snapshotPath, srcBytes, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	defer func() {
		_ = os.Remove(snapshotPath)
		_ = os.Remove(snapshotPath + ".wal")
	}()

	s.logger.Info("snapshot created at %s (%d MB)", snapshotPath, len(srcBytes)/(1024*1024))

	// Step 2: Attach snapshot READ_ONLY
	attacher, ok := s.target.(interface {
		AttachReadOnly(ctx context.Context, path string, alias string) error
		DetachDatabase(ctx context.Context, alias string) error
	})
	if !ok {
		return fmt.Errorf("target database does not support ATTACH")
	}

	if err := attacher.AttachReadOnly(ctx, snapshotPath, "upstream"); err != nil {
		return fmt.Errorf("failed to attach snapshot: %w", err)
	}

	// Step 3: List tables in source schema
	query := fmt.Sprintf(
		"SELECT table_name FROM upstream.information_schema.tables WHERE table_schema = '%s' AND table_type = 'BASE TABLE' ORDER BY table_name",
		sourceSchema)
	rows, err := s.target.QueryRows(ctx, query, 100)
	if err != nil {
		query = fmt.Sprintf(
			"SELECT table_name FROM duckdb_tables() WHERE database_name = 'upstream' AND schema_name = '%s' ORDER BY table_name",
			sourceSchema)
		rows, err = s.target.QueryRows(ctx, query, 100)
		if err != nil {
			_ = attacher.DetachDatabase(ctx, "upstream")
			return fmt.Errorf("failed to list source tables: %w", err)
		}
	}

	if len(rows) == 0 {
		_ = attacher.DetachDatabase(ctx, "upstream")
		return fmt.Errorf("no tables found in upstream.%s", sourceSchema)
	}

	// Step 4: Copy each table into v0.* and count rows from upstream
	var totalRows int64
	tableCount := 0
	for _, row := range rows {
		tableName := fmt.Sprintf("%v", row["table_name"])
		copySQL := fmt.Sprintf(
			"CREATE OR REPLACE TABLE v0.\"%s\" AS SELECT * FROM upstream.%s.\"%s\"",
			tableName, sourceSchema, tableName)
		s.logger.Info("refreshing v0.%s from %s.%s", tableName, sourceSchema, tableName)
		if err := s.target.ExecuteSQL(ctx, copySQL); err != nil {
			_ = attacher.DetachDatabase(ctx, "upstream")
			return fmt.Errorf("failed to copy %s.%s to v0: %w", sourceSchema, tableName, err)
		}

		// Count rows from upstream source (avoids schema ambiguity on target db)
		countQuery := fmt.Sprintf("SELECT COUNT(*) AS cnt FROM upstream.%s.\"%s\"", sourceSchema, tableName)
		countRows, countErr := s.target.QueryRows(ctx, countQuery, 1)
		if countErr == nil && len(countRows) > 0 {
			if cnt, ok := countRows[0]["cnt"]; ok {
				switch val := cnt.(type) {
				case int64:
					totalRows += val
					s.logger.Info("  v0.%s: %d rows", tableName, val)
				}
			}
		}
		tableCount++
	}

	// Step 5: Detach upstream before writing to _meta (avoids schema ambiguity)
	_ = attacher.DetachDatabase(ctx, "upstream")

	// Step 6: Look up latest merged epoch from state DB
	var epochID int64
	if s.state != nil {
		epoch, epochErr := s.state.GetLatestMergedEpoch(ctx)
		if epochErr == nil && epoch != nil {
			epochID = epoch.ID
		}
	}

	// Step 7: Record in _meta.refresh_log
	durationMs := time.Since(start).Milliseconds()
	logSQL := fmt.Sprintf(
		"INSERT INTO _meta.refresh_log (refreshed_at, source, epoch_id, tables, total_rows, duration_ms) VALUES (current_timestamp, '%s', %d, %d, %d, %d)",
		sourcePath, epochID, tableCount, totalRows, durationMs)
	if err := s.target.ExecuteSQL(ctx, logSQL); err != nil {
		s.logger.Warn("failed to record refresh in _meta.refresh_log: %v", err)
	}

	s.logger.Info("refresh complete: %d tables, %d total rows, %dms", tableCount, totalRows, durationMs)
	return nil
}
