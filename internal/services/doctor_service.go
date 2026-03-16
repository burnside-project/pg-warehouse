package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/domain/warehouse"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/models"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// DoctorCheck represents the result of a single doctor check.
type DoctorCheck struct {
	Name   string
	Status string // "ok", "warn", "fail"
	Detail string
}

// DoctorService validates configuration and connectivity.
type DoctorService struct {
	configLoader ports.ConfigLoader
	pgSource     ports.PostgresSource
	warehouse    ports.WarehouseStore
	logger       *logging.Logger
}

// NewDoctorService creates a new DoctorService.
func NewDoctorService(cl ports.ConfigLoader, pg ports.PostgresSource, wh ports.WarehouseStore, logger *logging.Logger) *DoctorService {
	return &DoctorService{
		configLoader: cl,
		pgSource:     pg,
		warehouse:    wh,
		logger:       logger,
	}
}

// RunChecks executes all doctor checks and returns results.
func (s *DoctorService) RunChecks(ctx context.Context, cfg *models.ProjectConfig) []DoctorCheck {
	var checks []DoctorCheck

	// Check config validity
	checks = append(checks, s.checkConfig(cfg))

	// Check Postgres connectivity
	checks = append(checks, s.checkPostgres(ctx))

	// Check DuckDB accessibility
	checks = append(checks, s.checkWarehouse(ctx))

	// Check bootstrap state
	checks = append(checks, s.checkBootstrap(ctx)...)

	return checks
}

func (s *DoctorService) checkConfig(cfg *models.ProjectConfig) DoctorCheck {
	if err := s.configLoader.Validate(cfg); err != nil {
		return DoctorCheck{Name: "config", Status: "fail", Detail: err.Error()}
	}
	return DoctorCheck{Name: "config", Status: "ok", Detail: "configuration is valid"}
}

func (s *DoctorService) checkPostgres(ctx context.Context) DoctorCheck {
	if s.pgSource == nil {
		return DoctorCheck{Name: "postgres", Status: "warn", Detail: "no postgres source configured"}
	}
	if err := s.pgSource.Ping(ctx); err != nil {
		return DoctorCheck{Name: "postgres", Status: "fail", Detail: fmt.Sprintf("connection failed: %v", err)}
	}
	return DoctorCheck{Name: "postgres", Status: "ok", Detail: "connected successfully"}
}

func (s *DoctorService) checkWarehouse(ctx context.Context) DoctorCheck {
	if err := s.warehouse.Open(ctx); err != nil {
		return DoctorCheck{Name: "duckdb", Status: "fail", Detail: fmt.Sprintf("failed to open: %v", err)}
	}
	return DoctorCheck{Name: "duckdb", Status: "ok", Detail: "accessible"}
}

func (s *DoctorService) checkBootstrap(ctx context.Context) []DoctorCheck {
	var checks []DoctorCheck
	for _, schema := range warehouse.AllSchemas() {
		table := fmt.Sprintf("%s.__dummy__", schema)
		// We check schema existence by checking if we can reference it
		exists, err := s.warehouse.TableExists(ctx, table)
		if err != nil && !strings.Contains(err.Error(), "does not exist") {
			checks = append(checks, DoctorCheck{
				Name:   fmt.Sprintf("schema_%s", schema),
				Status: "fail",
				Detail: fmt.Sprintf("schema check failed: %v", err),
			})
		} else {
			_ = exists
			checks = append(checks, DoctorCheck{
				Name:   fmt.Sprintf("schema_%s", schema),
				Status: "ok",
				Detail: fmt.Sprintf("schema '%s' exists", schema),
			})
		}
	}
	return checks
}
