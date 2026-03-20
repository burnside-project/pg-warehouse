package services

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/domain/silver"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// SilverService manages versioned schemas in silver.duckdb.
type SilverService struct {
	silver ports.WarehouseStore
	logger *logging.Logger
}

// NewSilverService creates a new SilverService.
func NewSilverService(silver ports.WarehouseStore, logger *logging.Logger) *SilverService {
	return &SilverService{silver: silver, logger: logger}
}

// CreateVersion creates a new versioned schema in silver.duckdb.
func (s *SilverService) CreateVersion(ctx context.Context, label string) (int, error) {
	// Get next version number from _meta.versions
	rows, err := s.silver.QueryRows(ctx, "SELECT COALESCE(MAX(version), 0) + 1 AS next FROM _meta.versions", 1)
	if err != nil {
		return 0, fmt.Errorf("query next version: %w", err)
	}
	nextVersion := 1
	if len(rows) > 0 {
		if v, ok := rows[0]["next"]; ok {
			switch val := v.(type) {
			case int64:
				nextVersion = int(val)
			case int32:
				nextVersion = int(val)
			case float64:
				nextVersion = int(val)
			}
		}
	}

	schemaName := silver.VersionSchemaName(nextVersion)

	// Create schema
	err = s.silver.ExecuteSQL(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		return 0, fmt.Errorf("create schema %s: %w", schemaName, err)
	}

	// Register version
	err = s.silver.ExecuteSQLWithArgs(ctx,
		"INSERT INTO _meta.versions (version, label, status) VALUES ($1, $2, $3)",
		nextVersion, label, models.SilverVersionExperiment)
	if err != nil {
		return 0, fmt.Errorf("register version: %w", err)
	}

	s.logger.Info("Created silver version %d (schema: %s, label: %s)", nextVersion, schemaName, label)
	return nextVersion, nil
}

// Promote swaps current.* views to point to the specified version.
func (s *SilverService) Promote(ctx context.Context, version int) error {
	schemaName := silver.VersionSchemaName(version)

	// Get all tables in the version schema
	query := fmt.Sprintf(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_type = 'BASE TABLE'",
		schemaName)
	rows, err := s.silver.QueryRows(ctx, query, 100)
	if err != nil {
		return fmt.Errorf("list tables in %s: %w", schemaName, err)
	}

	if len(rows) == 0 {
		return fmt.Errorf("version %d has no tables", version)
	}

	// Also check for views in the version schema (inherited tables)
	viewQuery := fmt.Sprintf(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_type = 'VIEW'",
		schemaName)
	viewRows, err := s.silver.QueryRows(ctx, viewQuery, 100)
	if err == nil {
		rows = append(rows, viewRows...)
	}

	// Create or replace views in current schema
	for _, row := range rows {
		tableName := fmt.Sprintf("%v", row["table_name"])
		sql := fmt.Sprintf(
			"CREATE OR REPLACE VIEW current.%s AS SELECT * FROM %s.%s",
			tableName, schemaName, tableName)
		if err := s.silver.ExecuteSQL(ctx, sql); err != nil {
			return fmt.Errorf("create view current.%s: %w", tableName, err)
		}
	}

	// Update version status
	err = s.silver.ExecuteSQL(ctx, fmt.Sprintf(
		"UPDATE _meta.versions SET status = '%s' WHERE status = '%s'",
		models.SilverVersionArchived, models.SilverVersionActive))
	if err != nil {
		return fmt.Errorf("archive previous version: %w", err)
	}

	err = s.silver.ExecuteSQL(ctx, fmt.Sprintf(
		"UPDATE _meta.versions SET status = '%s', promoted_at = current_timestamp WHERE version = %d",
		models.SilverVersionActive, version))
	if err != nil {
		return fmt.Errorf("promote version %d: %w", version, err)
	}

	s.logger.Info("Promoted silver version %d to production", version)
	return nil
}

// ListVersions returns all registered silver versions.
func (s *SilverService) ListVersions(ctx context.Context) ([]models.SilverVersion, error) {
	rows, err := s.silver.QueryRows(ctx,
		"SELECT version, label, status, epoch_id, description, created_at, promoted_at FROM _meta.versions ORDER BY version", 100)
	if err != nil {
		return nil, fmt.Errorf("list versions: %w", err)
	}

	var versions []models.SilverVersion
	for _, row := range rows {
		v := models.SilverVersion{
			Label:  fmt.Sprintf("%v", row["label"]),
			Status: fmt.Sprintf("%v", row["status"]),
		}
		if val, ok := row["version"]; ok {
			switch t := val.(type) {
			case int64:
				v.Version = int(t)
			case int32:
				v.Version = int(t)
			case float64:
				v.Version = int(t)
			}
		}
		versions = append(versions, v)
	}
	return versions, nil
}

// DropVersion drops an archived version schema.
func (s *SilverService) DropVersion(ctx context.Context, version int) error {
	// Verify it's archived
	rows, err := s.silver.QueryRows(ctx,
		fmt.Sprintf("SELECT status FROM _meta.versions WHERE version = %d", version), 1)
	if err != nil || len(rows) == 0 {
		return fmt.Errorf("version %d not found", version)
	}
	status := fmt.Sprintf("%v", rows[0]["status"])
	if status != models.SilverVersionArchived {
		return fmt.Errorf("cannot drop version %d: status is '%s' (must be 'archived')", version, status)
	}

	schemaName := silver.VersionSchemaName(version)
	err = s.silver.ExecuteSQL(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	if err != nil {
		return fmt.Errorf("drop schema %s: %w", schemaName, err)
	}

	err = s.silver.ExecuteSQL(ctx, fmt.Sprintf("DELETE FROM _meta.versions WHERE version = %d", version))
	if err != nil {
		return fmt.Errorf("delete version record: %w", err)
	}

	s.logger.Info("Dropped silver version %d", version)
	return nil
}
