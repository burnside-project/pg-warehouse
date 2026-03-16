package services

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// InitService handles project initialization.
type InitService struct {
	configLoader ports.ConfigLoader
	warehouse    ports.WarehouseStore
	pgSource     ports.PostgresSource
}

// NewInitService creates a new InitService.
func NewInitService(configLoader ports.ConfigLoader, warehouse ports.WarehouseStore, pgSource ports.PostgresSource) *InitService {
	return &InitService{
		configLoader: configLoader,
		warehouse:    warehouse,
		pgSource:     pgSource,
	}
}

// Init initializes the pg-warehouse project: opens DuckDB, bootstraps schemas, and optionally validates Postgres.
func (s *InitService) Init(ctx context.Context, validatePG bool) error {
	if err := s.warehouse.Open(ctx); err != nil {
		return fmt.Errorf("failed to open warehouse: %w", err)
	}

	if err := s.warehouse.Bootstrap(ctx); err != nil {
		return fmt.Errorf("failed to bootstrap warehouse: %w", err)
	}

	if validatePG && s.pgSource != nil {
		if err := s.pgSource.Ping(ctx); err != nil {
			return fmt.Errorf("postgres connectivity check failed: %w", err)
		}
	}

	return nil
}
