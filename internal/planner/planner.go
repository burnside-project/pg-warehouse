package planner

import (
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/domain/model"
	"github.com/burnside-project/pg-warehouse/internal/domain/release"
	"github.com/burnside-project/pg-warehouse/internal/graph"
)

// BuildPlan represents the ordered steps for a build.
type BuildPlan struct {
	ReleaseName    string
	ReleaseVersion string
	Steps          []BuildStep
}

// BuildStep represents a single model materialization.
type BuildStep struct {
	Model       *model.Model
	Order       int
	Layer       string
	TargetDB    string // "silver" or "feature"
	TargetTable string
}

// Plan generates a BuildPlan from a release, model map, and DAG.
func Plan(rel *release.Release, models map[string]*model.Model, dag *graph.DAG) (*BuildPlan, error) {
	// Build subgraph for release models.
	subDAG := dag.Select(rel.Models)

	sorted, err := subDAG.TopologicalSort()
	if err != nil {
		return nil, fmt.Errorf("graph resolution failed: %w", err)
	}

	plan := &BuildPlan{
		ReleaseName:    rel.Name,
		ReleaseVersion: rel.Version,
	}

	for i, node := range sorted {
		m, exists := models[node.Name]
		if !exists {
			return nil, fmt.Errorf("model %q referenced in release but not found", node.Name)
		}

		targetDB := "silver"
		if m.Layer == "features" || m.Layer == "feature" || m.Layer == "marts" {
			targetDB = "feature"
		}

		step := BuildStep{
			Model:       m,
			Order:       i + 1,
			Layer:       m.Layer,
			TargetDB:    targetDB,
			TargetTable: fmt.Sprintf("v1.%s", m.Name),
		}
		plan.Steps = append(plan.Steps, step)
	}

	return plan, nil
}
