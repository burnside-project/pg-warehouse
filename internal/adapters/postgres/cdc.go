package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CDCAdapter implements ports.CDCSource using PostgreSQL logical replication.
type CDCAdapter struct {
	pool     *pgxpool.Pool
	replConn *pgconn.PgConn
	decoder  *decoder
	url      string
}

// NewCDCAdapter creates a new CDC adapter. Pool is shared with Source for SQL operations.
func NewCDCAdapter(url string, pool *pgxpool.Pool) *CDCAdapter {
	return &CDCAdapter{
		pool:    pool,
		decoder: newDecoder(),
		url:     url,
	}
}

// Setup creates a publication and replication slot.
func (c *CDCAdapter) Setup(ctx context.Context, tables []string, publicationName string, slotName string) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Create publication
	tableList := strings.Join(tables, ", ")
	pubSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgx.Identifier{publicationName}.Sanitize(), tableList)

	if _, err := conn.Exec(ctx, pubSQL); err != nil {
		// Ignore if already exists
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create publication: %w", err)
		}
	}

	// Create replication slot using replication connection
	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect for replication: %w", err)
	}
	defer func() { _ = replConn.Close(ctx) }()

	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
	}

	return nil
}

// Teardown drops the publication and replication slot.
func (c *CDCAdapter) Teardown(ctx context.Context, publicationName string, slotName string) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Drop publication
	pubSQL := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{publicationName}.Sanitize())
	if _, err := conn.Exec(ctx, pubSQL); err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	// Drop replication slot
	slotSQL := fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName)
	if _, err := conn.Exec(ctx, slotSQL); err != nil {
		if !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("failed to drop replication slot: %w", err)
		}
	}

	return nil
}

// StartSnapshot performs an initial table copy.
func (c *CDCAdapter) StartSnapshot(ctx context.Context, table string) ([]map[string]any, []models.ColumnInfo, string, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Get current LSN as snapshot point
	var lsn string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
		return nil, nil, "", fmt.Errorf("failed to get current LSN: %w", err)
	}

	// Get schema
	parts := strings.SplitN(table, ".", 2)
	schema := "public"
	tableName := table
	if len(parts) == 2 {
		schema = parts[0]
		tableName = parts[1]
	}

	// Get column info
	colRows, err := conn.Query(ctx,
		`SELECT column_name, data_type, is_nullable, ordinal_position
		FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, schema, tableName)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get schema: %w", err)
	}

	var columns []models.ColumnInfo
	for colRows.Next() {
		var col models.ColumnInfo
		var nullable string
		if err := colRows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			colRows.Close()
			return nil, nil, "", err
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}
	colRows.Close()

	// Fetch all rows
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s.%s",
		pgx.Identifier{schema}.Sanitize(), pgx.Identifier{tableName}.Sanitize()))
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to fetch snapshot: %w", err)
	}

	fieldDescs := rows.FieldDescriptions()
	var result []map[string]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, "", fmt.Errorf("failed to scan row: %w", err)
		}
		row := make(map[string]any, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[string(fd.Name)] = values[i]
		}
		result = append(result, row)
	}

	return result, columns, lsn, rows.Err()
}

// Stream starts logical replication from the given LSN.
// The events channel is closed when Stream returns.
func (c *CDCAdapter) Stream(ctx context.Context, slotName string, publicationName string, startLSN string, events chan<- ports.CDCEvent) error {
	defer close(events)

	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect for streaming: %w", err)
	}
	c.replConn = replConn
	defer func() {
		c.replConn = nil
		_ = replConn.Close(context.Background())
	}()

	lsn, err := pglogrepl.ParseLSN(startLSN)
	if err != nil {
		return fmt.Errorf("failed to parse start LSN: %w", err)
	}

	err = pglogrepl.StartReplication(ctx, replConn, slotName, lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", publicationName),
				"messages 'true'",
			},
		})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	// Send immediate standby status to prevent wal_sender_timeout on idle streams.
	// PostgreSQL default wal_sender_timeout is 60s; without this, a stream with no
	// initial events would be killed before the first periodic status update.
	_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn})

	standbyDeadline := time.Now().Add(10 * time.Second)
	// Track the highest WAL position seen from the server (keepalives + xlog data).
	// Confirming this position tells PostgreSQL it can release WAL up to this point,
	// preventing unbounded WAL accumulation when subscribed tables are idle but other
	// tables in the database are actively written to. This is essential for any
	// customer workload regardless of which tables are in the publication.
	serverWALEnd := lsn

	for {
		if ctx.Err() != nil {
			// Confirm final position before exit
			_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: serverWALEnd})
			return ctx.Err()
		}

		// Send standby status periodically — confirms the latest server WAL position
		// to release WAL and prevent wal_sender_timeout
		if time.Now().After(standbyDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: serverWALEnd})
			if err != nil {
				return fmt.Errorf("failed to send standby status: %w", err)
			}
			standbyDeadline = time.Now().Add(10 * time.Second)
		}

		recvCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := replConn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive message error: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("replication error: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse keepalive: %w", err)
			}
			// Advance confirmed position to the server's WAL end.
			// This releases WAL from unrelated tables, preventing lag buildup.
			if pkm.ServerWALEnd > serverWALEnd {
				serverWALEnd = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				standbyDeadline = time.Time{} // force immediate reply
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse xlog data: %w", err)
			}

			walEnd := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if walEnd > serverWALEnd {
				serverWALEnd = walEnd
			}

			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
			if err != nil {
				return fmt.Errorf("failed to parse logical message: %w", err)
			}

			cdcEvent, err := c.processMessage(logicalMsg, xld.WALStart.String(), xld.ServerTime)
			if err != nil {
				return fmt.Errorf("failed to process message: %w", err)
			}
			if cdcEvent != nil {
				select {
				case events <- *cdcEvent:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// ConfirmLSN sends a standby status update.
func (c *CDCAdapter) ConfirmLSN(ctx context.Context, lsn string) error {
	if c.replConn == nil {
		return nil
	}
	parsedLSN, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return fmt.Errorf("failed to parse LSN: %w", err)
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.replConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: parsedLSN})
}

// Status returns the current replication slot status.
func (c *CDCAdapter) Status(ctx context.Context, slotName string) (*ports.CDCStatus, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var status ports.CDCStatus
	status.SlotName = slotName

	err = conn.QueryRow(ctx, `
		SELECT plugin, active, COALESCE(confirmed_flush_lsn::text, '0/0'),
			pg_current_wal_lsn()::text,
			COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)
		FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(
		&status.Plugin, &status.Active, &status.ConfirmedLSN,
		&status.CurrentLSN, &status.LagBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get slot status: %w", err)
	}

	return &status, nil
}

// Close releases CDC connection resources.
func (c *CDCAdapter) Close() error {
	if c.replConn != nil {
		return c.replConn.Close(context.Background())
	}
	return nil
}

func (c *CDCAdapter) connectReplication(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(c.url)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["replication"] = "database"

	return pgconn.ConnectConfig(ctx, cfg)
}

func (c *CDCAdapter) processMessage(msg pglogrepl.Message, lsn string, serverTime time.Time) (*ports.CDCEvent, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		c.decoder.handleRelation(m)
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		return c.decoder.decodeInsert(m, lsn, serverTime)

	case *pglogrepl.UpdateMessageV2:
		return c.decoder.decodeUpdate(m, lsn, serverTime)

	case *pglogrepl.DeleteMessageV2:
		return c.decoder.decodeDelete(m, lsn, serverTime)

	case *pglogrepl.BeginMessage:
		return nil, nil

	case *pglogrepl.CommitMessage:
		return nil, nil

	case *pglogrepl.TruncateMessageV2:
		return nil, nil

	case *pglogrepl.TypeMessageV2:
		return nil, nil

	case *pglogrepl.OriginMessage:
		return nil, nil

	default:
		return nil, nil
	}
}
