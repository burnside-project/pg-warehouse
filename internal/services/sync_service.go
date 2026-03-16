package services

import (
	"context"
	"fmt"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/domain/sync"
	"github.com/burnside-project/pg-warehouse/internal/domain/warehouse"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/burnside-project/pg-warehouse/internal/util"
)

// SyncService handles table synchronization from PostgreSQL to DuckDB.
type SyncService struct {
	pgSource  ports.PostgresSource
	warehouse ports.WarehouseStore
	metadata  ports.MetadataStore
	logger    *logging.Logger
}

// NewSyncService creates a new SyncService.
func NewSyncService(pgSource ports.PostgresSource, wh ports.WarehouseStore, meta ports.MetadataStore, logger *logging.Logger) *SyncService {
	return &SyncService{
		pgSource:  pgSource,
		warehouse: wh,
		metadata:  meta,
		logger:    logger,
	}
}

// SyncAll synchronizes all configured tables.
func (s *SyncService) SyncAll(ctx context.Context, tables []models.TableConfig, batchSize int) ([]models.SyncResult, error) {
	var results []models.SyncResult

	for _, table := range tables {
		result := s.syncTable(ctx, table, batchSize)
		results = append(results, result)
		if result.Error != nil {
			s.logger.Error("sync failed for table %s: %v", table.Name, result.Error)
		} else {
			s.logger.Info("synced table %s: mode=%s, inserted=%d", table.Name, result.Mode, result.InsertedRows)
		}
	}

	return results, nil
}

func (s *SyncService) syncTable(ctx context.Context, table models.TableConfig, batchSize int) models.SyncResult {
	startTime := time.Now()
	runID := util.NewRunID()

	// Get current sync state to determine mode
	state, err := s.metadata.GetSyncState(ctx, table.Name)
	if err != nil {
		return models.SyncResult{TableName: table.Name, Error: fmt.Errorf("failed to get sync state: %w", err)}
	}

	lastWatermark := ""
	if state != nil {
		lastWatermark = state.LastWatermark
	}

	mode := sync.DetermineMode(table.WatermarkColumn, lastWatermark)

	// Record sync history start
	history := &models.SyncHistory{
		RunID:     runID,
		TableName: table.Name,
		SyncMode:  string(mode),
		StartedAt: startTime,
		Status:    "running",
	}
	_ = s.metadata.InsertSyncHistory(ctx, history)

	// Fetch column schema for type-aware table creation
	columns, err := s.pgSource.GetTableSchema(ctx, table.Name)
	if err != nil {
		s.logger.Warn("could not get table schema for %s, falling back to VARCHAR: %v", table.Name, err)
		columns = nil
	}

	var rows []map[string]any
	switch mode {
	case sync.SyncModeFull:
		rows, err = s.pgSource.FetchFull(ctx, table.Name, batchSize)
	case sync.SyncModeIncremental:
		rows, err = s.pgSource.FetchIncremental(ctx, table.Name, table.WatermarkColumn, lastWatermark, batchSize)
	}

	if err != nil {
		s.finalizeSyncHistory(ctx, history, 0, "failed", err.Error())
		return models.SyncResult{TableName: table.Name, Mode: string(mode), Error: err, Duration: time.Since(startTime)}
	}

	// Write to warehouse
	rawTable := warehouse.RawTableName(table.Name)
	if mode == sync.SyncModeFull {
		err = s.warehouse.CreateTableFromRows(ctx, rawTable, rows, columns)
	} else {
		// For incremental: stage then merge
		stageTable := warehouse.StageTableName(table.Name)
		if err = s.warehouse.CreateTableFromRows(ctx, stageTable, rows, columns); err == nil {
			err = s.warehouse.MergeStageToRaw(ctx, stageTable, rawTable, table.PrimaryKey)
		}
	}

	if err != nil {
		s.finalizeSyncHistory(ctx, history, 0, "failed", err.Error())
		return models.SyncResult{TableName: table.Name, Mode: string(mode), Error: err, Duration: time.Since(startTime)}
	}

	insertedRows := int64(len(rows))

	// Update sync state
	now := time.Now().UTC()
	newState := &models.SyncState{
		TableName:       table.Name,
		SyncMode:        string(mode),
		WatermarkColumn: table.WatermarkColumn,
		LastSyncAt:      &now,
		LastStatus:      "success",
		RowCount:        insertedRows,
	}
	if mode == sync.SyncModeFull {
		newState.LastSnapshotAt = &now
	}
	_ = s.metadata.UpsertSyncState(ctx, newState)

	s.finalizeSyncHistory(ctx, history, insertedRows, "success", "")

	return models.SyncResult{
		TableName:    table.Name,
		Mode:         string(mode),
		InsertedRows: insertedRows,
		Duration:     time.Since(startTime),
	}
}

func (s *SyncService) finalizeSyncHistory(ctx context.Context, history *models.SyncHistory, insertedRows int64, status string, errMsg string) {
	now := time.Now().UTC()
	history.FinishedAt = &now
	history.InsertedRows = insertedRows
	history.Status = status
	history.ErrorMessage = errMsg
	_ = s.metadata.InsertSyncHistory(ctx, history)
}
