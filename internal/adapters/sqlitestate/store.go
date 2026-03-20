package sqlitestate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/models"

	_ "modernc.org/sqlite"
)

// Store implements ports.StateStore using SQLite.
type Store struct {
	db   *sql.DB
	path string
}

// NewStore creates a new SQLite state store.
// For in-memory testing, pass ":memory:" as path.
func NewStore(path string) (*Store, error) {
	// Ensure directory exists
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create state directory: %w", err)
		}
	}

	dsn := path
	if path != ":memory:" {
		dsn = path + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open state db: %w", err)
	}

	// Single-writer serialization (collector-agent pattern)
	db.SetMaxOpenConns(1)

	ctx := context.Background()

	// Bootstrap schema
	if _, err := db.ExecContext(ctx, bootstrapSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to bootstrap state db: %w", err)
	}

	// Run migrations
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to migrate state db: %w", err)
	}

	return &Store{db: db, path: path}, nil
}

// Close releases the SQLite connection.
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// --- MetadataStore interface ---

// GetSyncState returns the current sync state for a table.
func (s *Store) GetSyncState(ctx context.Context, table string) (*models.SyncState, error) {
	query := `SELECT table_name, sync_mode, watermark_column, last_watermark, last_lsn,
		last_snapshot_at, last_sync_at, last_status, row_count, error_message
		FROM sync_state WHERE table_name = ?`

	var state models.SyncState
	var snapshotAt, syncAt sql.NullString
	err := s.db.QueryRowContext(ctx, query, table).Scan(
		&state.TableName, &state.SyncMode, &state.WatermarkColumn,
		&state.LastWatermark, &state.LastLSN, &snapshotAt,
		&syncAt, &state.LastStatus, &state.RowCount, &state.ErrorMessage,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get sync state: %w", err)
	}
	state.LastSnapshotAt = parseNullTime(snapshotAt)
	state.LastSyncAt = parseNullTime(syncAt)
	return &state, nil
}

// GetAllSyncStates returns sync state for all tables.
func (s *Store) GetAllSyncStates(ctx context.Context) ([]models.SyncState, error) {
	query := `SELECT table_name, sync_mode, watermark_column, last_watermark, last_lsn,
		last_snapshot_at, last_sync_at, last_status, row_count, error_message
		FROM sync_state ORDER BY table_name`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all sync states: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var states []models.SyncState
	for rows.Next() {
		var state models.SyncState
		var snapshotAt, syncAt sql.NullString
		if err := rows.Scan(
			&state.TableName, &state.SyncMode, &state.WatermarkColumn,
			&state.LastWatermark, &state.LastLSN, &snapshotAt,
			&syncAt, &state.LastStatus, &state.RowCount, &state.ErrorMessage,
		); err != nil {
			return nil, fmt.Errorf("failed to scan sync state: %w", err)
		}
		state.LastSnapshotAt = parseNullTime(snapshotAt)
		state.LastSyncAt = parseNullTime(syncAt)
		states = append(states, state)
	}
	return states, rows.Err()
}

// UpsertSyncState creates or updates the sync state for a table.
func (s *Store) UpsertSyncState(ctx context.Context, state *models.SyncState) error {
	query := `INSERT INTO sync_state (table_name, sync_mode, watermark_column, last_watermark, last_lsn, last_snapshot_at, last_sync_at, last_status, row_count, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(table_name) DO UPDATE SET
			sync_mode = excluded.sync_mode,
			watermark_column = excluded.watermark_column,
			last_watermark = excluded.last_watermark,
			last_lsn = excluded.last_lsn,
			last_snapshot_at = excluded.last_snapshot_at,
			last_sync_at = excluded.last_sync_at,
			last_status = excluded.last_status,
			row_count = excluded.row_count,
			error_message = excluded.error_message`

	_, err := s.db.ExecContext(ctx, query,
		state.TableName, state.SyncMode, state.WatermarkColumn,
		state.LastWatermark, state.LastLSN, formatTime(state.LastSnapshotAt),
		formatTime(state.LastSyncAt), state.LastStatus, state.RowCount, state.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert sync state: %w", err)
	}
	return nil
}

// InsertSyncHistory records a sync history entry.
func (s *Store) InsertSyncHistory(ctx context.Context, history *models.SyncHistory) error {
	query := `INSERT INTO sync_history (run_id, table_name, sync_mode, started_at, finished_at, inserted_rows, updated_rows, deleted_rows, status, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		history.RunID, history.TableName, history.SyncMode,
		history.StartedAt.Format(time.RFC3339), formatTime(history.FinishedAt),
		history.InsertedRows, history.UpdatedRows, history.DeletedRows,
		history.Status, history.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to insert sync history: %w", err)
	}
	return nil
}

// InsertFeatureRun records a feature run entry.
func (s *Store) InsertFeatureRun(ctx context.Context, run *models.FeatureRun) error {
	query := `INSERT INTO feature_runs (run_id, sql_file, target_table, output_path, output_type, started_at, finished_at, row_count, status, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		run.RunID, run.SQLFile, run.TargetTable,
		run.OutputPath, run.OutputType, run.StartedAt.Format(time.RFC3339),
		formatTime(run.FinishedAt), run.RowCount, run.Status, run.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to insert feature run: %w", err)
	}
	return nil
}

// UpdateFeatureRun updates an existing feature run entry.
func (s *Store) UpdateFeatureRun(ctx context.Context, run *models.FeatureRun) error {
	query := `UPDATE feature_runs SET
		finished_at = ?, row_count = ?, status = ?, error_message = ?
		WHERE run_id = ?`

	_, err := s.db.ExecContext(ctx, query,
		formatTime(run.FinishedAt), run.RowCount, run.Status, run.ErrorMessage, run.RunID,
	)
	if err != nil {
		return fmt.Errorf("failed to update feature run: %w", err)
	}
	return nil
}

// --- StateStore extensions ---

// GetProjectIdentity returns the project identity.
func (s *Store) GetProjectIdentity(ctx context.Context) (*models.ProjectIdentity, error) {
	query := `SELECT project_name, pg_url_hash, warehouse_path, created_at FROM project_identity WHERE id = 1`
	var id models.ProjectIdentity
	var createdAt string
	err := s.db.QueryRowContext(ctx, query).Scan(&id.ProjectName, &id.PGURLHash, &id.WarehousePath, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get project identity: %w", err)
	}
	id.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return &id, nil
}

// SaveProjectIdentity saves the project identity (singleton upsert).
func (s *Store) SaveProjectIdentity(ctx context.Context, id *models.ProjectIdentity) error {
	query := `INSERT INTO project_identity (id, project_name, pg_url_hash, warehouse_path, created_at)
		VALUES (1, ?, ?, ?, datetime('now'))
		ON CONFLICT(id) DO UPDATE SET
			project_name = excluded.project_name,
			pg_url_hash = excluded.pg_url_hash,
			warehouse_path = excluded.warehouse_path`

	_, err := s.db.ExecContext(ctx, query, id.ProjectName, id.PGURLHash, id.WarehousePath)
	if err != nil {
		return fmt.Errorf("failed to save project identity: %w", err)
	}
	return nil
}

// GetCDCState returns the CDC state for a table.
func (s *Store) GetCDCState(ctx context.Context, table string) (*models.CDCState, error) {
	query := `SELECT table_name, slot_name, publication_name, confirmed_lsn, last_received_lsn, status, error_message, updated_at
		FROM cdc_state WHERE table_name = ?`

	var state models.CDCState
	var updatedAt string
	err := s.db.QueryRowContext(ctx, query, table).Scan(
		&state.TableName, &state.SlotName, &state.PublicationName,
		&state.ConfirmedLSN, &state.LastReceivedLSN, &state.Status,
		&state.ErrorMessage, &updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get CDC state: %w", err)
	}
	state.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	return &state, nil
}

// GetAllCDCStates returns CDC state for all tables.
func (s *Store) GetAllCDCStates(ctx context.Context) ([]models.CDCState, error) {
	query := `SELECT table_name, slot_name, publication_name, confirmed_lsn, last_received_lsn, status, error_message, updated_at
		FROM cdc_state ORDER BY table_name`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all CDC states: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var states []models.CDCState
	for rows.Next() {
		var state models.CDCState
		var updatedAt string
		if err := rows.Scan(
			&state.TableName, &state.SlotName, &state.PublicationName,
			&state.ConfirmedLSN, &state.LastReceivedLSN, &state.Status,
			&state.ErrorMessage, &updatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan CDC state: %w", err)
		}
		state.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		states = append(states, state)
	}
	return states, rows.Err()
}

// UpsertCDCState creates or updates the CDC state for a table.
func (s *Store) UpsertCDCState(ctx context.Context, state *models.CDCState) error {
	query := `INSERT INTO cdc_state (table_name, slot_name, publication_name, confirmed_lsn, last_received_lsn, status, error_message, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
		ON CONFLICT(table_name) DO UPDATE SET
			slot_name = excluded.slot_name,
			publication_name = excluded.publication_name,
			confirmed_lsn = excluded.confirmed_lsn,
			last_received_lsn = excluded.last_received_lsn,
			status = excluded.status,
			error_message = excluded.error_message,
			updated_at = datetime('now')`

	_, err := s.db.ExecContext(ctx, query,
		state.TableName, state.SlotName, state.PublicationName,
		state.ConfirmedLSN, state.LastReceivedLSN, state.Status, state.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert CDC state: %w", err)
	}
	return nil
}

// AddAuditEntry appends a bounded audit log entry.
func (s *Store) AddAuditEntry(ctx context.Context, level, event, message string, metadata map[string]any) error {
	var metaJSON string
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			metaJSON = "{}"
		} else {
			metaJSON = string(b)
		}
	}

	query := `INSERT INTO audit_log (level, event, message, metadata) VALUES (?, ?, ?, ?)`
	_, err := s.db.ExecContext(ctx, query, level, event, message, metaJSON)
	if err != nil {
		return fmt.Errorf("failed to add audit entry: %w", err)
	}
	return nil
}

// GetRecentAuditEntries returns the most recent audit log entries.
func (s *Store) GetRecentAuditEntries(ctx context.Context, limit int) ([]models.AuditEntry, error) {
	query := `SELECT id, timestamp, level, event, COALESCE(message, ''), COALESCE(metadata, '')
		FROM audit_log ORDER BY id DESC LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get audit entries: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var entries []models.AuditEntry
	for rows.Next() {
		var e models.AuditEntry
		var ts string
		if err := rows.Scan(&e.ID, &ts, &e.Level, &e.Event, &e.Message, &e.Metadata); err != nil {
			return nil, fmt.Errorf("failed to scan audit entry: %w", err)
		}
		e.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetWatermark returns a named watermark.
func (s *Store) GetWatermark(ctx context.Context, name string) (*models.Watermark, error) {
	query := `SELECT name, value, updated_at FROM watermarks WHERE name = ?`
	var w models.Watermark
	var updatedAt string
	err := s.db.QueryRowContext(ctx, query, name).Scan(&w.Name, &w.Value, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get watermark: %w", err)
	}
	w.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	return &w, nil
}

// SetWatermark creates or updates a named watermark.
func (s *Store) SetWatermark(ctx context.Context, name string, value string) error {
	query := `INSERT INTO watermarks (name, value, updated_at) VALUES (?, ?, datetime('now'))
		ON CONFLICT(name) DO UPDATE SET value = excluded.value, updated_at = datetime('now')`
	_, err := s.db.ExecContext(ctx, query, name, value)
	if err != nil {
		return fmt.Errorf("failed to set watermark: %w", err)
	}
	return nil
}

// TryAcquireLock attempts to acquire an execution lock.
func (s *Store) TryAcquireLock(ctx context.Context, pid int, hostname string, ttl time.Duration) (bool, error) {
	expiresAt := time.Now().UTC().Add(ttl)

	// Check existing lock
	var existingExpires sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT expires_at FROM lock_state WHERE id = 1`).Scan(&existingExpires)
	if err == nil && existingExpires.Valid {
		expires, _ := time.Parse(time.RFC3339, existingExpires.String)
		if time.Now().UTC().Before(expires) {
			return false, nil // lock held by another process
		}
	}

	// Acquire or replace expired lock
	query := `INSERT INTO lock_state (id, holder_pid, holder_host, acquired_at, expires_at)
		VALUES (1, ?, ?, datetime('now'), ?)
		ON CONFLICT(id) DO UPDATE SET
			holder_pid = excluded.holder_pid,
			holder_host = excluded.holder_host,
			acquired_at = datetime('now'),
			expires_at = excluded.expires_at`
	_, err = s.db.ExecContext(ctx, query, pid, hostname, expiresAt.Format(time.RFC3339))
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}
	return true, nil
}

// ReleaseLock releases the execution lock.
func (s *Store) ReleaseLock(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM lock_state WHERE id = 1`)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	return nil
}

// GetLockState returns the current lock state.
func (s *Store) GetLockState(ctx context.Context) (*models.LockState, error) {
	query := `SELECT holder_pid, holder_host, acquired_at, expires_at FROM lock_state WHERE id = 1`
	var lock models.LockState
	var acquiredAt, expiresAt string
	err := s.db.QueryRowContext(ctx, query).Scan(&lock.HolderPID, &lock.HolderHost, &acquiredAt, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get lock state: %w", err)
	}
	lock.AcquiredAt, _ = time.Parse(time.RFC3339, acquiredAt)
	lock.ExpiresAt, _ = time.Parse(time.RFC3339, expiresAt)
	return &lock, nil
}

// SchemaVersion returns the current schema version.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	err := s.db.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE id = 1`).Scan(&version)
	if err != nil {
		return 0, nil // no version yet
	}
	return version, nil
}

// --- Epoch methods ---

// OpenEpoch inserts a new epoch with status 'open' and returns it.
func (s *Store) OpenEpoch(ctx context.Context) (*models.Epoch, error) {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO epochs (status) VALUES ('open')`)
	if err != nil {
		return nil, fmt.Errorf("failed to open epoch: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get epoch id: %w", err)
	}

	// Read back the full row
	var epoch models.Epoch
	var startedAt string
	err = s.db.QueryRowContext(ctx,
		`SELECT id, started_at, start_lsn, end_lsn, row_count, status FROM epochs WHERE id = ?`, id).
		Scan(&epoch.ID, &startedAt, &epoch.StartLSN, &epoch.EndLSN, &epoch.RowCount, &epoch.Status)
	if err != nil {
		return nil, fmt.Errorf("failed to read back epoch: %w", err)
	}
	epoch.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAt)
	return &epoch, nil
}

// CommitEpoch marks an epoch as committed with the given end LSN and row count.
func (s *Store) CommitEpoch(ctx context.Context, epochID int64, endLSN string, rowCount int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE epochs SET status = 'committed', committed_at = datetime('now'), end_lsn = ?, row_count = ? WHERE id = ?`,
		endLSN, rowCount, epochID)
	if err != nil {
		return fmt.Errorf("failed to commit epoch %d: %w", epochID, err)
	}
	return nil
}

// MarkEpochMerged marks an epoch as merged.
func (s *Store) MarkEpochMerged(ctx context.Context, epochID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE epochs SET status = 'merged' WHERE id = ?`, epochID)
	if err != nil {
		return fmt.Errorf("failed to mark epoch %d as merged: %w", epochID, err)
	}
	return nil
}

// GetOpenEpoch returns the most recent open epoch, or nil if none exists.
func (s *Store) GetOpenEpoch(ctx context.Context) (*models.Epoch, error) {
	var epoch models.Epoch
	var startedAt string
	var committedAt sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, started_at, committed_at, start_lsn, end_lsn, row_count, status
		 FROM epochs WHERE status = 'open' ORDER BY id DESC LIMIT 1`).
		Scan(&epoch.ID, &startedAt, &committedAt, &epoch.StartLSN, &epoch.EndLSN, &epoch.RowCount, &epoch.Status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get open epoch: %w", err)
	}
	epoch.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAt)
	epoch.CommittedAt = parseNullTimePtr(committedAt)
	return &epoch, nil
}

// GetCommittedEpochs returns all committed epochs ordered by id ascending.
func (s *Store) GetCommittedEpochs(ctx context.Context) ([]models.Epoch, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, started_at, committed_at, start_lsn, end_lsn, row_count, status
		 FROM epochs WHERE status = 'committed' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("failed to get committed epochs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var epochs []models.Epoch
	for rows.Next() {
		var epoch models.Epoch
		var startedAt string
		var committedAt sql.NullString
		if err := rows.Scan(&epoch.ID, &startedAt, &committedAt, &epoch.StartLSN, &epoch.EndLSN, &epoch.RowCount, &epoch.Status); err != nil {
			return nil, fmt.Errorf("failed to scan epoch: %w", err)
		}
		epoch.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAt)
		epoch.CommittedAt = parseNullTimePtr(committedAt)
		epochs = append(epochs, epoch)
	}
	return epochs, rows.Err()
}

// GetLatestMergedEpoch returns the most recently merged epoch, or nil if none exists.
func (s *Store) GetLatestMergedEpoch(ctx context.Context) (*models.Epoch, error) {
	var epoch models.Epoch
	var startedAt string
	var committedAt sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, started_at, committed_at, start_lsn, end_lsn, row_count, status
		 FROM epochs WHERE status = 'merged' ORDER BY id DESC LIMIT 1`).
		Scan(&epoch.ID, &startedAt, &committedAt, &epoch.StartLSN, &epoch.EndLSN, &epoch.RowCount, &epoch.Status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest merged epoch: %w", err)
	}
	epoch.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAt)
	epoch.CommittedAt = parseNullTimePtr(committedAt)
	return &epoch, nil
}

// --- Helpers ---

func formatTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339)
}

func parseNullTimePtr(s sql.NullString) *time.Time {
	if !s.Valid || s.String == "" {
		return nil
	}
	t, err := time.Parse("2006-01-02 15:04:05", s.String)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s.String)
		if err != nil {
			return nil
		}
	}
	return &t
}

func parseNullTime(s sql.NullString) *time.Time {
	if !s.Valid || s.String == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s.String)
	if err != nil {
		// Try alternate format
		t, err = time.Parse("2006-01-02 15:04:05", s.String)
		if err != nil {
			return nil
		}
	}
	return &t
}
