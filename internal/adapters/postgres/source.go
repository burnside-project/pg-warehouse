package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Source implements ports.PostgresSource using pgxpool.
type Source struct {
	pool         *pgxpool.Pool
	url          string
	queryTimeout time.Duration
}

// NewSource creates a new PostgreSQL source adapter backed by a pgxpool connection pool.
// It accepts the full PostgresCfg to apply max_conns (capped at 5), connect_timeout, and query_timeout.
func NewSource(pgCfg models.PostgresCfg) (*Source, error) {
	cfg, err := pgxpool.ParseConfig(pgCfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres connection config: %w", err)
	}

	// Apply max_conns, capped at 5 per configuration contract.
	maxConns := pgCfg.MaxConns
	if maxConns <= 0 {
		maxConns = 2
	}
	if maxConns > 5 {
		maxConns = 5
	}
	cfg.MaxConns = int32(maxConns)
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	// Apply connect_timeout.
	if pgCfg.ConnectTimeout != "" {
		if d, err := time.ParseDuration(pgCfg.ConnectTimeout); err == nil {
			cfg.ConnConfig.ConnectTimeout = d
		}
	}

	// Parse query_timeout for use in query contexts.
	var queryTimeout time.Duration
	if pgCfg.QueryTimeout != "" {
		if d, err := time.ParseDuration(pgCfg.QueryTimeout); err == nil {
			queryTimeout = d
		}
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	return &Source{pool: pool, url: pgCfg.URL, queryTimeout: queryTimeout}, nil
}

// Ping checks connectivity to PostgreSQL.
func (s *Source) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// ListTables returns all table names in the given schema.
func (s *Source) ListTables(ctx context.Context, schema string) ([]string, error) {
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name`
	rows, err := s.pool.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// GetTableSchema returns column metadata for a table.
func (s *Source) GetTableSchema(ctx context.Context, table string) ([]models.ColumnInfo, error) {
	parts := strings.SplitN(table, ".", 2)
	schema := "public"
	tableName := table
	if len(parts) == 2 {
		schema = parts[0]
		tableName = parts[1]
	}

	query := `SELECT column_name, data_type, is_nullable, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`
	rows, err := s.pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	var columns []models.ColumnInfo
	for rows.Next() {
		var col models.ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// FetchFull reads all rows from a table.
func (s *Source) FetchFull(ctx context.Context, table string, batchSize int) ([]map[string]any, error) {
	query := fmt.Sprintf("SELECT * FROM %s", quoteTable(table))
	return s.fetchRows(ctx, query)
}

// FetchIncremental reads rows where watermarkColumn > lastWatermark, limited by batchSize.
func (s *Source) FetchIncremental(ctx context.Context, table string, watermarkColumn string, lastWatermark string, batchSize int) ([]map[string]any, error) {
	quotedWM := pgx.Identifier{watermarkColumn}.Sanitize()
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT $2", quoteTable(table), quotedWM, quotedWM)
	return s.fetchRowsWithArgs(ctx, query, lastWatermark, batchSize)
}

// Pool returns the underlying connection pool for sharing with other adapters (e.g. CDC).
func (s *Source) Pool() *pgxpool.Pool {
	return s.pool
}

// Close releases the PostgreSQL connection pool.
func (s *Source) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// quoteTable safely quotes a possibly schema-qualified table name.
func quoteTable(table string) string {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return pgx.Identifier{parts[0], parts[1]}.Sanitize()
	}
	return pgx.Identifier{table}.Sanitize()
}

func (s *Source) fetchRows(ctx context.Context, query string) ([]map[string]any, error) {
	return s.fetchRowsWithArgs(ctx, query)
}

func (s *Source) fetchRowsWithArgs(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		columns[i] = fd.Name
	}

	var result []map[string]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
