package services

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/domain/warehouse"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// CDCService handles CDC lifecycle management.
type CDCService struct {
	cdcSource ports.CDCSource
	warehouse ports.WarehouseStore
	state     ports.StateStore
	pgSource  ports.PostgresSource
	logger    *logging.Logger

	// Epoch tracking for CDC writes
	cdcCfg        models.CDCCfg
	currentEpoch  *models.Epoch
	epochRowCount int64
}

// NewCDCService creates a new CDCService.
func NewCDCService(cdc ports.CDCSource, wh ports.WarehouseStore, state ports.StateStore, pg ports.PostgresSource, logger *logging.Logger) *CDCService {
	return &CDCService{
		cdcSource: cdc,
		warehouse: wh,
		state:     state,
		pgSource:  pg,
		logger:    logger,
	}
}

// Setup creates the publication and replication slot.
func (s *CDCService) Setup(ctx context.Context, cfg models.CDCCfg) error {
	s.logger.Info("setting up CDC: publication=%s slot=%s tables=%v", cfg.PublicationName, cfg.SlotName, cfg.Tables)

	if err := s.cdcSource.Setup(ctx, cfg.Tables, cfg.PublicationName, cfg.SlotName); err != nil {
		_ = s.state.AddAuditEntry(ctx, models.AuditError, models.EventCDCSetup,
			fmt.Sprintf("CDC setup failed: %v", err), nil)
		return fmt.Errorf("CDC setup failed: %w", err)
	}

	for _, table := range cfg.Tables {
		cdcState := &models.CDCState{
			TableName:       table,
			SlotName:        cfg.SlotName,
			PublicationName: cfg.PublicationName,
			Status:          "stopped",
		}
		if err := s.state.UpsertCDCState(ctx, cdcState); err != nil {
			return fmt.Errorf("failed to save CDC state: %w", err)
		}
	}

	_ = s.state.AddAuditEntry(ctx, models.AuditInfo, models.EventCDCSetup,
		"CDC setup complete", map[string]any{
			"publication": cfg.PublicationName,
			"slot":        cfg.SlotName,
			"tables":      cfg.Tables,
		})

	s.logger.Info("CDC setup complete")
	return nil
}

// Teardown drops the publication and replication slot.
func (s *CDCService) Teardown(ctx context.Context, cfg models.CDCCfg) error {
	s.logger.Info("tearing down CDC: publication=%s slot=%s", cfg.PublicationName, cfg.SlotName)

	if err := s.cdcSource.Teardown(ctx, cfg.PublicationName, cfg.SlotName); err != nil {
		return fmt.Errorf("CDC teardown failed: %w", err)
	}

	_ = s.state.AddAuditEntry(ctx, models.AuditInfo, models.EventCDCTeardown,
		"CDC teardown complete", nil)

	s.logger.Info("CDC teardown complete")
	return nil
}

// Status returns the current CDC replication status.
func (s *CDCService) Status(ctx context.Context, cfg models.CDCCfg) (*ports.CDCStatus, []models.CDCState, error) {
	status, err := s.cdcSource.Status(ctx, cfg.SlotName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get slot status: %w", err)
	}

	states, err := s.state.GetAllCDCStates(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get CDC states: %w", err)
	}

	return status, states, nil
}

// Start begins the CDC streaming process.
// If fromLSN is non-empty, the initial snapshot is skipped and all tables are
// set to that LSN. This supports fast-seed workflows where DuckDB has been
// pre-populated via bulk copy (pg_dump, COPY TO, or DuckDB postgres_scan).
func (s *CDCService) Start(ctx context.Context, cfg models.CDCCfg, tableConfigs []models.TableConfig, fromLSN string) error {
	pid := os.Getpid()
	hostname, _ := os.Hostname()

	acquired, err := s.state.TryAcquireLock(ctx, pid, hostname, 24*time.Hour)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		lockState, _ := s.state.GetLockState(ctx)
		if lockState != nil {
			return fmt.Errorf("another pg-warehouse process is running (PID %d on %s since %s)",
				lockState.HolderPID, lockState.HolderHost, lockState.AcquiredAt.Format(time.RFC3339))
		}
		return fmt.Errorf("failed to acquire execution lock")
	}
	defer func() { _ = s.state.ReleaseLock(ctx) }()

	_ = s.state.AddAuditEntry(ctx, models.AuditInfo, models.EventCDCStart,
		"CDC streaming started", map[string]any{"slot": cfg.SlotName})

	// Store CDC config for epoch management
	s.cdcCfg = cfg

	tableConfigMap := make(map[string]models.TableConfig)
	for _, tc := range tableConfigs {
		tableConfigMap[tc.Name] = tc
	}

	// Phase 1: Fast-seed mode — skip snapshot, set all tables to the provided LSN
	if fromLSN != "" {
		s.logger.Info("fast-seed mode: setting all tables to LSN %s, skipping snapshot", fromLSN)
		for _, table := range cfg.Tables {
			newState := &models.CDCState{
				TableName:       table,
				SlotName:        cfg.SlotName,
				PublicationName: cfg.PublicationName,
				ConfirmedLSN:    fromLSN,
				Status:          "snapshot",
			}
			if err := s.state.UpsertCDCState(ctx, newState); err != nil {
				return fmt.Errorf("failed to set CDC state for %s: %w", table, err)
			}
		}
		_ = s.state.AddAuditEntry(ctx, models.AuditInfo, "cdc.fast_seed",
			fmt.Sprintf("fast-seed: set %d tables to LSN %s", len(cfg.Tables), fromLSN), nil)
	} else {
		// Phase 1 (normal): Initial snapshot for tables not yet synced
		for _, table := range cfg.Tables {
			cdcState, err := s.state.GetCDCState(ctx, table)
			if err != nil {
				return fmt.Errorf("failed to get CDC state for %s: %w", table, err)
			}

			if cdcState == nil || cdcState.ConfirmedLSN == "" {
				s.logger.Info("performing initial snapshot for %s", table)

				rows, columns, snapshotLSN, err := s.cdcSource.StartSnapshot(ctx, table)
				if err != nil {
					return fmt.Errorf("snapshot failed for %s: %w", table, err)
				}

				parts := strings.SplitN(table, ".", 2)
				rawName := table
				if len(parts) == 2 {
					rawName = parts[1]
				}
				rawTable := warehouse.RawTableName(rawName)

				if err := s.warehouse.CreateTableFromRows(ctx, rawTable, rows, columns); err != nil {
					return fmt.Errorf("failed to write snapshot for %s: %w", table, err)
				}

				newState := &models.CDCState{
					TableName:       table,
					SlotName:        cfg.SlotName,
					PublicationName: cfg.PublicationName,
					ConfirmedLSN:    snapshotLSN,
					Status:          "snapshot",
				}
				if err := s.state.UpsertCDCState(ctx, newState); err != nil {
					return fmt.Errorf("failed to update CDC state: %w", err)
				}

				s.logger.Info("snapshot complete for %s: %d rows, LSN=%s", table, len(rows), snapshotLSN)
			}
		}
	}

	// Phase 2: Determine start LSN
	startLSN := ""
	allStates, err := s.state.GetAllCDCStates(ctx)
	if err != nil {
		return fmt.Errorf("failed to get CDC states: %w", err)
	}
	for _, cs := range allStates {
		if cs.ConfirmedLSN != "" {
			if startLSN == "" || cs.ConfirmedLSN < startLSN {
				startLSN = cs.ConfirmedLSN
			}
		}
	}
	if startLSN == "" {
		startLSN = "0/0"
	}

	// Phase 3: Stream changes with reconnection
	s.logger.Info("starting CDC stream from LSN %s", startLSN)

	for _, table := range cfg.Tables {
		cs := &models.CDCState{
			TableName:       table,
			SlotName:        cfg.SlotName,
			PublicationName: cfg.PublicationName,
			ConfirmedLSN:    startLSN,
			Status:          "streaming",
		}
		_ = s.state.UpsertCDCState(ctx, cs)
	}

	currentLSN := startLSN
	const maxRetries = 10
	backoff := 1 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt > 0 {
			s.logger.Info("reconnecting CDC stream (attempt %d/%d) from LSN %s in %s",
				attempt, maxRetries, currentLSN, backoff)
			_ = s.state.AddAuditEntry(ctx, models.AuditInfo, "cdc.reconnect",
				fmt.Sprintf("reconnecting attempt %d from LSN %s", attempt, currentLSN), nil)

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}

			// Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s max
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}

		lastLSN, err := s.streamOnce(ctx, cfg, currentLSN, tableConfigMap)
		if lastLSN != "" {
			currentLSN = lastLSN
		}

		if err == nil || ctx.Err() != nil {
			// Graceful shutdown or clean exit — persist final state
			if currentLSN != "" {
				bgCtx := context.Background()
				_ = s.cdcSource.ConfirmLSN(bgCtx, currentLSN)
				_ = s.state.SetWatermark(bgCtx, "cdc_confirmed_lsn", currentLSN)
			}
			_ = s.state.AddAuditEntry(context.Background(), models.AuditInfo, models.EventCDCStop,
				"CDC streaming stopped", nil)
			return err
		}

		// Recoverable error — log and retry
		s.logger.Error("CDC stream error: %v", err)
		_ = s.state.AddAuditEntry(ctx, models.AuditError, models.EventCDCError,
			fmt.Sprintf("CDC stream error (will retry): %v", err), nil)

		// Reset backoff on successful progress
		if lastLSN != "" {
			backoff = 1 * time.Second
			attempt = 0 // reset retry counter on progress
		}
	}

	_ = s.state.AddAuditEntry(context.Background(), models.AuditError, models.EventCDCStop,
		fmt.Sprintf("CDC stopped after %d retries", maxRetries), nil)
	return fmt.Errorf("CDC stream failed after %d reconnection attempts", maxRetries)
}

// streamOnce runs a single streaming session. Returns the last processed LSN and any error.
// On recoverable errors (EOF, connection reset), the caller can retry from the returned LSN.
func (s *CDCService) streamOnce(ctx context.Context, cfg models.CDCCfg, startLSN string, tableConfigMap map[string]models.TableConfig) (string, error) {
	events := make(chan ports.CDCEvent, 1000)
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.cdcSource.Stream(ctx, cfg.SlotName, cfg.PublicationName, startLSN, events)
	}()

	var batch []ports.CDCEvent
	flushTicker := time.NewTicker(1 * time.Second)
	defer flushTicker.Stop()

	confirmTicker := time.NewTicker(10 * time.Second)
	defer confirmTicker.Stop()

	// Health check ticker for lag monitoring
	healthInterval := time.Duration(cfg.HealthCheckSec) * time.Second
	if healthInterval <= 0 {
		healthInterval = 60 * time.Second
	}
	healthTicker := time.NewTicker(healthInterval)
	defer healthTicker.Stop()

	var lastLSN string

	for {
		select {
		case event, ok := <-events:
			if !ok {
				if len(batch) > 0 {
					s.flushBatch(ctx, batch, tableConfigMap)
				}
				return lastLSN, <-errCh
			}
			batch = append(batch, event)
			lastLSN = event.LSN

			if len(batch) >= 100 {
				s.flushBatch(ctx, batch, tableConfigMap)
				batch = batch[:0]
			}

		case <-flushTicker.C:
			if len(batch) > 0 {
				s.flushBatch(ctx, batch, tableConfigMap)
				batch = batch[:0]
			}

		case <-confirmTicker.C:
			if lastLSN != "" {
				if err := s.cdcSource.ConfirmLSN(ctx, lastLSN); err != nil {
					s.logger.Error("failed to confirm LSN: %v", err)
				} else {
					_ = s.state.SetWatermark(ctx, "cdc_confirmed_lsn", lastLSN)
				}

				// Epoch management: commit if interval or row threshold reached
				s.commitEpochIfNeeded(ctx, lastLSN, tableConfigMap)
			}

		case <-healthTicker.C:
			if err := s.checkReplicationHealth(ctx, cfg); err != nil {
				// Lag exceeded threshold — return error to trigger shutdown
				if len(batch) > 0 {
					s.flushBatch(ctx, batch, tableConfigMap)
				}
				return lastLSN, err
			}

		case err := <-errCh:
			if len(batch) > 0 {
				s.flushBatch(ctx, batch, tableConfigMap)
			}
			return lastLSN, err
		}
	}
}

// checkReplicationHealth queries the replication slot lag and returns an error
// if it exceeds the configured MaxLagBytes threshold. This prevents the CDC
// process from running when the PostgreSQL server is at risk of filling its disk
// with retained WAL segments.
func (s *CDCService) checkReplicationHealth(ctx context.Context, cfg models.CDCCfg) error {
	if cfg.MaxLagBytes <= 0 {
		return nil // disabled
	}

	status, err := s.cdcSource.Status(ctx, cfg.SlotName)
	if err != nil {
		s.logger.Warn("health check: failed to query replication status: %v", err)
		return nil // don't kill CDC over a transient status query failure
	}

	if status.LagBytes > cfg.MaxLagBytes {
		msg := fmt.Sprintf(
			"CRITICAL: replication lag (%d bytes / %.1f GB) exceeds max_lag_bytes (%d bytes / %.1f GB). "+
				"Stopping CDC to prevent PostgreSQL disk fill. "+
				"Investigate and resolve before restarting.",
			status.LagBytes, float64(status.LagBytes)/(1024*1024*1024),
			cfg.MaxLagBytes, float64(cfg.MaxLagBytes)/(1024*1024*1024))

		s.logger.Error("%s", msg)
		_ = s.state.AddAuditEntry(ctx, "critical", "cdc.lag_exceeded", msg, map[string]any{
			"lag_bytes":     status.LagBytes,
			"max_lag_bytes": cfg.MaxLagBytes,
			"slot":          cfg.SlotName,
		})

		return fmt.Errorf("replication lag exceeded threshold: %d bytes > %d bytes", status.LagBytes, cfg.MaxLagBytes)
	}

	// Log healthy status at debug level
	s.logger.Debug("health check: lag=%d bytes (%.1f MB), threshold=%d bytes",
		status.LagBytes, float64(status.LagBytes)/(1024*1024), cfg.MaxLagBytes)

	return nil
}

func (s *CDCService) flushBatch(ctx context.Context, batch []ports.CDCEvent, tableConfigs map[string]models.TableConfig) {
	// Ensure we have an open epoch for stamping rows
	if err := s.ensureOpenEpoch(ctx); err != nil {
		s.logger.Error("failed to ensure open epoch: %v", err)
		return
	}

	for _, event := range batch {
		parts := strings.SplitN(event.Table, ".", 2)
		tableName := event.Table
		if len(parts) == 2 {
			tableName = parts[1]
		}
		stageTable := warehouse.StageTableName(tableName)

		var err error
		switch event.Operation {
		case "INSERT":
			if event.NewTuple != nil {
				row := s.injectEpochMetadata(event.NewTuple, false)
				err = s.warehouse.InsertRows(ctx, stageTable, []map[string]any{row})
				if err == nil {
					s.epochRowCount++
				}
			}
		case "UPDATE":
			if event.NewTuple != nil {
				row := s.injectEpochMetadata(event.NewTuple, false)
				err = s.warehouse.InsertRows(ctx, stageTable, []map[string]any{row})
				if err == nil {
					s.epochRowCount++
				}
			}
		case "DELETE":
			// Write tombstone row with _deleted=true to stage
			tc, ok := tableConfigs[event.Table]
			if ok && len(tc.PrimaryKey) > 0 && event.OldTuple != nil {
				row := s.injectEpochMetadata(event.OldTuple, true)
				err = s.warehouse.InsertRows(ctx, stageTable, []map[string]any{row})
				if err == nil {
					s.epochRowCount++
				}
			}
		}

		if err != nil {
			s.logger.Error("CDC apply failed for %s %s: %v", event.Operation, event.Table, err)
		}
	}
}

// injectEpochMetadata copies the tuple and adds _epoch and _deleted metadata columns.
func (s *CDCService) injectEpochMetadata(tuple map[string]any, deleted bool) map[string]any {
	row := make(map[string]any, len(tuple)+2)
	for k, v := range tuple {
		row[k] = v
	}
	row[warehouse.ColEpoch] = s.currentEpoch.ID
	row[warehouse.ColDeleted] = deleted
	return row
}

// ensureOpenEpoch gets or creates an open epoch for the current CDC session.
func (s *CDCService) ensureOpenEpoch(ctx context.Context) error {
	if s.currentEpoch != nil {
		return nil
	}

	// Try to resume an existing open epoch
	epoch, err := s.state.GetOpenEpoch(ctx)
	if err != nil {
		return fmt.Errorf("failed to get open epoch: %w", err)
	}
	if epoch != nil {
		s.currentEpoch = epoch
		s.epochRowCount = epoch.RowCount
		return nil
	}

	// Open a new epoch
	epoch, err = s.state.OpenEpoch(ctx)
	if err != nil {
		return fmt.Errorf("failed to open new epoch: %w", err)
	}
	s.currentEpoch = epoch
	s.epochRowCount = 0
	s.logger.Info("opened new epoch %d", epoch.ID)
	return nil
}

// commitEpochIfNeeded checks if the current epoch should be committed based on
// elapsed time or row count thresholds, and if so commits it and merges any
// committed epochs.
func (s *CDCService) commitEpochIfNeeded(ctx context.Context, lastLSN string, tableConfigs map[string]models.TableConfig) {
	if s.currentEpoch == nil {
		return
	}

	elapsed := time.Since(s.currentEpoch.StartedAt)
	intervalExceeded := elapsed >= time.Duration(s.cdcCfg.EpochIntervalSec)*time.Second
	rowsExceeded := s.epochRowCount >= int64(s.cdcCfg.EpochMaxRows)

	if !intervalExceeded && !rowsExceeded {
		return
	}

	// Commit the current epoch
	epochID := s.currentEpoch.ID
	if err := s.state.CommitEpoch(ctx, epochID, lastLSN, s.epochRowCount); err != nil {
		s.logger.Error("failed to commit epoch %d: %v", epochID, err)
		return
	}
	s.logger.Info("committed epoch %d (rows=%d, elapsed=%s)", epochID, s.epochRowCount, elapsed.Round(time.Second))

	// Clear current epoch so a new one is opened on next flush
	s.currentEpoch = nil
	s.epochRowCount = 0

	// Merge committed epochs into raw
	s.mergeCommittedEpochs(ctx, tableConfigs)
}

// mergeCommittedEpochs iterates over all committed epochs and merges each one
// from stage to raw for every configured table, then marks the epoch as merged.
func (s *CDCService) mergeCommittedEpochs(ctx context.Context, tableConfigs map[string]models.TableConfig) {
	epochs, err := s.state.GetCommittedEpochs(ctx)
	if err != nil {
		s.logger.Error("failed to get committed epochs: %v", err)
		return
	}

	for _, epoch := range epochs {
		merged := true
		for _, tc := range tableConfigs {
			tableName := tc.Name
			parts := strings.SplitN(tableName, ".", 2)
			if len(parts) == 2 {
				tableName = parts[1]
			}
			stageTable := warehouse.StageTableName(tableName)
			rawTable := warehouse.RawTableName(tableName)

			if err := s.warehouse.MergeStageToRawForEpoch(ctx, stageTable, rawTable, tc.PrimaryKey, epoch.ID); err != nil {
				s.logger.Error("failed to merge epoch %d for table %s: %v", epoch.ID, tc.Name, err)
				merged = false
				break
			}
		}

		if merged {
			if err := s.state.MarkEpochMerged(ctx, epoch.ID); err != nil {
				s.logger.Error("failed to mark epoch %d as merged: %v", epoch.ID, err)
			} else {
				s.logger.Info("merged epoch %d", epoch.ID)
			}
		}
	}
}

// TeardownSlot drops the replication slot to prevent orphaned WAL accumulation.
// This should be called on CDC exit (graceful or crash) when DropSlotOnExit is enabled.
func (s *CDCService) TeardownSlot(ctx context.Context, cfg models.CDCCfg) {
	s.logger.Info("dropping replication slot %s (drop_slot_on_exit enabled)", cfg.SlotName)
	if err := s.cdcSource.Teardown(ctx, cfg.PublicationName, cfg.SlotName); err != nil {
		s.logger.Error("failed to drop slot %s on exit: %v", cfg.SlotName, err)
		_ = s.state.AddAuditEntry(ctx, "error", "cdc.slot_drop_failed",
			fmt.Sprintf("failed to drop slot on exit: %v", err), nil)
	} else {
		s.logger.Info("dropped replication slot %s successfully", cfg.SlotName)
		_ = s.state.AddAuditEntry(ctx, "info", "cdc.slot_dropped",
			fmt.Sprintf("slot %s dropped on exit to prevent WAL accumulation", cfg.SlotName), nil)
	}
}

