package config

import (
	"testing"

	"github.com/burnside-project/pg-warehouse/internal/models"
)

func TestValidate(t *testing.T) {
	validCfg := &models.ProjectConfig{
		Postgres: models.PostgresCfg{URL: "postgres://localhost/test"},
		DuckDB:   models.DuckDBCfg{Path: "./test.duckdb"},
		Sync: models.SyncCfg{
			Tables: []models.TableConfig{
				{Name: "orders", PrimaryKey: []string{"id"}},
			},
		},
	}

	if err := Validate(validCfg); err != nil {
		t.Errorf("expected valid config, got error: %v", err)
	}

	// Missing postgres URL
	badPG := *validCfg
	badPG.Postgres.URL = ""
	if err := Validate(&badPG); err == nil {
		t.Error("expected error for missing postgres URL")
	}

	// Missing duckdb path
	badDB := *validCfg
	badDB.DuckDB.Path = ""
	if err := Validate(&badDB); err == nil {
		t.Error("expected error for missing duckdb path")
	}

	// No tables
	badTables := *validCfg
	badTables.Sync.Tables = nil
	if err := Validate(&badTables); err == nil {
		t.Error("expected error for no tables")
	}

	// Table without primary key
	badPK := *validCfg
	badPK.Sync.Tables = []models.TableConfig{{Name: "orders"}}
	if err := Validate(&badPK); err == nil {
		t.Error("expected error for missing primary key")
	}
}
