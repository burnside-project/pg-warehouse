package sqlitestate

import (
	"context"
	"testing"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

func TestNewStore(t *testing.T) {
	store := NewTestStore(t)
	v, err := store.SchemaVersion(context.Background())
	if err != nil {
		t.Fatalf("failed to get schema version: %v", err)
	}
	if v != currentSchemaVersion {
		t.Errorf("expected schema version %d, got %d", currentSchemaVersion, v)
	}
}

func TestSyncState_CRUD(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// Get non-existent state
	state, err := store.GetSyncState(ctx, "public.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != nil {
		t.Error("expected nil for non-existent state")
	}

	// Upsert
	now := time.Now().UTC()
	newState := &models.SyncState{
		TableName:       "public.orders",
		SyncMode:        "full",
		WatermarkColumn: "updated_at",
		LastSyncAt:      &now,
		LastStatus:      "success",
		RowCount:        100,
	}
	if err := store.UpsertSyncState(ctx, newState); err != nil {
		t.Fatalf("failed to upsert: %v", err)
	}

	// Get
	state, err = store.GetSyncState(ctx, "public.orders")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if state == nil {
		t.Fatal("expected state, got nil")
	}
	if state.RowCount != 100 {
		t.Errorf("expected 100 rows, got %d", state.RowCount)
	}
	if state.LastStatus != "success" {
		t.Errorf("expected success, got %s", state.LastStatus)
	}

	// GetAll
	states, err := store.GetAllSyncStates(ctx)
	if err != nil {
		t.Fatalf("failed to get all: %v", err)
	}
	if len(states) != 1 {
		t.Errorf("expected 1 state, got %d", len(states))
	}
}

func TestSyncHistory_Insert(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	history := &models.SyncHistory{
		RunID:     "run_abc123",
		TableName: "public.orders",
		SyncMode:  "full",
		StartedAt: time.Now().UTC(),
		Status:    "running",
	}
	if err := store.InsertSyncHistory(ctx, history); err != nil {
		t.Fatalf("failed to insert history: %v", err)
	}
}

func TestFeatureRun_CRUD(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	run := &models.FeatureRun{
		RunID:       "run_feat123",
		SQLFile:     "features.sql",
		TargetTable: "feat.customers",
		StartedAt:   time.Now().UTC(),
		Status:      "running",
	}
	if err := store.InsertFeatureRun(ctx, run); err != nil {
		t.Fatalf("failed to insert feature run: %v", err)
	}

	now := time.Now().UTC()
	run.FinishedAt = &now
	run.RowCount = 50
	run.Status = "success"
	if err := store.UpdateFeatureRun(ctx, run); err != nil {
		t.Fatalf("failed to update feature run: %v", err)
	}
}

func TestProjectIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	id, err := store.GetProjectIdentity(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != nil {
		t.Error("expected nil for empty identity")
	}

	newID := &models.ProjectIdentity{
		ProjectName:   "test_project",
		PGURLHash:     "abc123hash",
		WarehousePath: "./test.duckdb",
	}
	if err := store.SaveProjectIdentity(ctx, newID); err != nil {
		t.Fatalf("failed to save identity: %v", err)
	}

	id, err = store.GetProjectIdentity(ctx)
	if err != nil {
		t.Fatalf("failed to get identity: %v", err)
	}
	if id.ProjectName != "test_project" {
		t.Errorf("expected test_project, got %s", id.ProjectName)
	}
}

func TestCDCState_CRUD(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	state := &models.CDCState{
		TableName:       "public.orders",
		SlotName:        "pgwh_slot",
		PublicationName: "pgwh_pub",
		ConfirmedLSN:    "0/16B3748",
		Status:          "streaming",
	}
	if err := store.UpsertCDCState(ctx, state); err != nil {
		t.Fatalf("failed to upsert CDC state: %v", err)
	}

	got, err := store.GetCDCState(ctx, "public.orders")
	if err != nil {
		t.Fatalf("failed to get CDC state: %v", err)
	}
	if got.ConfirmedLSN != "0/16B3748" {
		t.Errorf("expected LSN 0/16B3748, got %s", got.ConfirmedLSN)
	}

	all, err := store.GetAllCDCStates(ctx)
	if err != nil {
		t.Fatalf("failed to get all CDC states: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("expected 1 CDC state, got %d", len(all))
	}
}

func TestAuditLog(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	if err := store.AddAuditEntry(ctx, "info", "sync.start", "starting sync", map[string]any{"table": "orders"}); err != nil {
		t.Fatalf("failed to add audit entry: %v", err)
	}

	entries, err := store.GetRecentAuditEntries(ctx, 10)
	if err != nil {
		t.Fatalf("failed to get entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Event != "sync.start" {
		t.Errorf("expected sync.start, got %s", entries[0].Event)
	}
}

func TestAuditLog_BoundedCleanup(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// Insert 1010 entries to trigger cleanup (threshold is 1000)
	for i := 0; i < 1010; i++ {
		if err := store.AddAuditEntry(ctx, "info", "test", "entry", nil); err != nil {
			t.Fatalf("failed to add entry %d: %v", i, err)
		}
	}

	entries, err := store.GetRecentAuditEntries(ctx, 2000)
	if err != nil {
		t.Fatalf("failed to get entries: %v", err)
	}
	// Should be around 910 (1010 - 100 deleted by trigger)
	if len(entries) > 1000 {
		t.Errorf("expected bounded entries <= 1000, got %d", len(entries))
	}
}

func TestWatermarks(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	w, err := store.GetWatermark(ctx, "last_sync_lsn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w != nil {
		t.Error("expected nil for non-existent watermark")
	}

	if err := store.SetWatermark(ctx, "last_sync_lsn", "0/16B3748"); err != nil {
		t.Fatalf("failed to set watermark: %v", err)
	}

	w, err = store.GetWatermark(ctx, "last_sync_lsn")
	if err != nil {
		t.Fatalf("failed to get watermark: %v", err)
	}
	if w.Value != "0/16B3748" {
		t.Errorf("expected 0/16B3748, got %s", w.Value)
	}
}

func TestLock(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// Acquire lock
	acquired, err := store.TryAcquireLock(ctx, 12345, "localhost", 1*time.Minute)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	if !acquired {
		t.Error("expected to acquire lock")
	}

	// Try to acquire again - should fail
	acquired, err = store.TryAcquireLock(ctx, 99999, "other-host", 1*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if acquired {
		t.Error("expected lock to be held")
	}

	// Release
	if err := store.ReleaseLock(ctx); err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	// Should acquire again
	acquired, err = store.TryAcquireLock(ctx, 99999, "other-host", 1*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acquired {
		t.Error("expected to acquire lock after release")
	}

	// Check lock state
	lockState, err := store.GetLockState(ctx)
	if err != nil {
		t.Fatalf("failed to get lock state: %v", err)
	}
	if lockState == nil {
		t.Fatal("expected lock state")
	}
	if lockState.HolderPID != 99999 {
		t.Errorf("expected PID 99999, got %d", lockState.HolderPID)
	}
}

func TestLock_ExpiredLockCanBeAcquired(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)

	// Acquire with very short TTL
	acquired, err := store.TryAcquireLock(ctx, 111, "host1", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire")
	}

	// Wait for expiry
	time.Sleep(5 * time.Millisecond)

	// Another process should be able to acquire
	acquired, err = store.TryAcquireLock(ctx, 222, "host2", 1*time.Minute)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !acquired {
		t.Error("expected to acquire expired lock")
	}
}
