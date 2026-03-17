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
func (s *CDCService) Start(ctx context.Context, cfg models.CDCCfg, tableConfigs []models.TableConfig) error {
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

	tableConfigMap := make(map[string]models.TableConfig)
	for _, tc := range tableConfigs {
		tableConfigMap[tc.Name] = tc
	}

	// Phase 1: Initial snapshot for tables not yet synced
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
			}

		case err := <-errCh:
			if len(batch) > 0 {
				s.flushBatch(ctx, batch, tableConfigMap)
			}
			return lastLSN, err
		}
	}
}

func (s *CDCService) flushBatch(ctx context.Context, batch []ports.CDCEvent, tableConfigs map[string]models.TableConfig) {
	for _, event := range batch {
		parts := strings.SplitN(event.Table, ".", 2)
		rawName := event.Table
		if len(parts) == 2 {
			rawName = parts[1]
		}
		rawTable := warehouse.RawTableName(rawName)

		var err error
		switch event.Operation {
		case "INSERT":
			if event.NewTuple != nil {
				err = s.warehouse.InsertRows(ctx, rawTable, []map[string]any{event.NewTuple})
			}
		case "UPDATE":
			tc, ok := tableConfigs[event.Table]
			if ok && len(tc.PrimaryKey) > 0 && event.NewTuple != nil {
				deleteSQL, deleteArgs := buildParameterizedDelete(rawTable, tc.PrimaryKey, event.NewTuple)
				if deleteSQL != "" {
					_ = s.warehouse.ExecuteSQLWithArgs(ctx, deleteSQL, deleteArgs...)
				}
				err = s.warehouse.InsertRows(ctx, rawTable, []map[string]any{event.NewTuple})
			}
		case "DELETE":
			tc, ok := tableConfigs[event.Table]
			if ok && len(tc.PrimaryKey) > 0 && event.OldTuple != nil {
				deleteSQL, deleteArgs := buildParameterizedDelete(rawTable, tc.PrimaryKey, event.OldTuple)
				if deleteSQL != "" {
					err = s.warehouse.ExecuteSQLWithArgs(ctx, deleteSQL, deleteArgs...)
				}
			}
		}

		if err != nil {
			s.logger.Error("CDC apply failed for %s %s: %v", event.Operation, event.Table, err)
		}
	}
}

// buildParameterizedDelete builds a parameterized DELETE statement to prevent SQL injection.
func buildParameterizedDelete(table string, primaryKeys []string, tuple map[string]any) (string, []any) {
	var conditions []string
	var args []any
	for _, pk := range primaryKeys {
		if val, exists := tuple[pk]; exists {
			args = append(args, val)
			conditions = append(conditions, fmt.Sprintf("%s = $%d", pk, len(args)))
		}
	}
	if len(conditions) == 0 {
		return "", nil
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(conditions, " AND ")), args
}
