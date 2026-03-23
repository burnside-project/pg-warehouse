package registry

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/domain/contract"
	"github.com/burnside-project/pg-warehouse/internal/domain/model"
	"github.com/burnside-project/pg-warehouse/internal/domain/release"
	"github.com/burnside-project/pg-warehouse/internal/logging"
	"github.com/burnside-project/pg-warehouse/internal/ports"
)

// Registry manages contracts, models, releases, builds, and promotions in _meta.
type Registry struct {
	db     ports.WarehouseStore
	logger *logging.Logger
}

func NewRegistry(db ports.WarehouseStore, logger *logging.Logger) *Registry {
	return &Registry{db: db, logger: logger}
}

func (r *Registry) RegisterContract(ctx context.Context, c *contract.Contract) error {
	schemaJSON, _ := json.Marshal(c.Columns)
	pkJSON, _ := json.Marshal(c.PrimaryKey)
	sql := fmt.Sprintf(
		"DELETE FROM _meta.contracts WHERE contract_name = '%s' AND version = %d; "+
			"INSERT INTO _meta.contracts (contract_name, version, layer, schema_json, grain, primary_key, owner, status, file_path) "+
			"VALUES ('%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
		c.Name, c.Version,
		c.Name, c.Version, c.Layer, string(schemaJSON), c.Grain, string(pkJSON), c.Owner, c.Status, c.FilePath)
	return r.db.ExecuteSQL(ctx, sql)
}

func (r *Registry) RegisterModel(ctx context.Context, m *model.Model) error {
	depsJSON, _ := json.Marshal(m.DependsOn)
	srcsJSON, _ := json.Marshal(m.Sources)
	tagsJSON, _ := json.Marshal(m.Tags)
	sql := fmt.Sprintf(
		"DELETE FROM _meta.models WHERE model_name = '%s'; "+
			"INSERT INTO _meta.models (model_name, file_path, checksum, materialization, layer, depends_on_json, sources_json, contract_name, tags_json) "+
			"VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
		m.Name,
		m.Name, m.FilePath, m.Checksum, m.Materialization, m.Layer, string(depsJSON), string(srcsJSON), m.Contract, string(tagsJSON))
	return r.db.ExecuteSQL(ctx, sql)
}

func (r *Registry) RegisterRelease(ctx context.Context, rel *release.Release) error {
	modelsJSON, _ := json.Marshal(rel.Models)
	inputJSON, _ := json.Marshal(rel.Input)
	sql := fmt.Sprintf(
		"DELETE FROM _meta.releases WHERE release_name = '%s' AND version = '%s'; "+
			"INSERT INTO _meta.releases (release_name, version, description, models_json, input_json) "+
			"VALUES ('%s', '%s', '%s', '%s', '%s')",
		rel.Name, rel.Version,
		rel.Name, rel.Version, rel.Description, string(modelsJSON), string(inputJSON))
	return r.db.ExecuteSQL(ctx, sql)
}

func (r *Registry) StartBuild(ctx context.Context, releaseName, version, gitCommit string, epoch int64, env string) (int64, error) {
	sql := fmt.Sprintf(
		"INSERT INTO _meta.builds (release_name, release_version, git_commit, input_epoch, environment, status) "+
			"VALUES ('%s', '%s', '%s', %d, '%s', 'running')",
		releaseName, version, gitCommit, epoch, env)
	if err := r.db.ExecuteSQL(ctx, sql); err != nil {
		return 0, err
	}
	// Get the auto-generated build_id
	rows, err := r.db.QueryRows(ctx, "SELECT MAX(build_id) AS id FROM _meta.builds", 1)
	if err != nil || len(rows) == 0 {
		return 0, fmt.Errorf("failed to get build_id")
	}
	if id, ok := rows[0]["id"]; ok {
		switch v := id.(type) {
		case int64:
			return v, nil
		case int32:
			return int64(v), nil
		case float64:
			return int64(v), nil
		}
	}
	return 0, nil
}

func (r *Registry) FinishBuild(ctx context.Context, buildID int64, status string, durationMs int64, modelCount int, rowCount int64, errMsg string) error {
	sql := fmt.Sprintf(
		"UPDATE _meta.builds SET status = '%s', finished_at = current_timestamp, duration_ms = %d, model_count = %d, row_count = %d, error_message = '%s' WHERE build_id = %d",
		status, durationMs, modelCount, rowCount, errMsg, buildID)
	return r.db.ExecuteSQL(ctx, sql)
}

func (r *Registry) RecordPromotion(ctx context.Context, releaseName, version, env string, buildID int64, promotedBy string) error {
	sql := fmt.Sprintf(
		"DELETE FROM _meta.promotions WHERE release_name = '%s' AND release_version = '%s' AND environment = '%s'; "+
			"INSERT INTO _meta.promotions (release_name, release_version, environment, build_id, promoted_by) "+
			"VALUES ('%s', '%s', '%s', %d, '%s')",
		releaseName, version, env,
		releaseName, version, env, buildID, promotedBy)
	return r.db.ExecuteSQL(ctx, sql)
}
