package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/burnside-project/pg-warehouse/internal/build"
	"github.com/burnside-project/pg-warehouse/internal/domain/model"
	"github.com/burnside-project/pg-warehouse/internal/domain/release"
	"github.com/burnside-project/pg-warehouse/internal/graph"
	"github.com/burnside-project/pg-warehouse/internal/parser"
	"github.com/burnside-project/pg-warehouse/internal/planner"
	"github.com/burnside-project/pg-warehouse/internal/registry"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var (
	buildRelease string
	buildVersion string
	buildSelect  string
)

var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Run a graph-resolved build for models or a release",
	Long: `Build materializes outputs using DAG-resolved model execution.

Modes:
  build                          Build ALL models in models/ directory (DAG order)
  build --select model_name      Build a single model and its transitive dependencies
  build --release NAME --version VER  Build only models listed in a release file

Examples:
  pg-warehouse build
  pg-warehouse build --select customer_ltv
  pg-warehouse build --release default --version 0.1.0`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// 1. Discover all models
		models := make(map[string]*model.Model)
		dag := graph.NewDAG()

		modelDirs := []string{
			filepath.Join(app.Cfg.Paths.Models, "silver"),
			filepath.Join(app.Cfg.Paths.Models, "marts"),
			filepath.Join(app.Cfg.Paths.Models, "features"),
		}
		for _, dir := range modelDirs {
			files, _ := filepath.Glob(filepath.Join(dir, "*.sql"))
			layer := filepath.Base(dir)
			for _, f := range files {
				result, pErr := parser.ParseFile(f)
				if pErr != nil {
					continue
				}
				name := result.Name
				if name == "" {
					name = stripNumericPrefix(stripExt(filepath.Base(f)))
				}
				checksum, _ := parser.Checksum(f)

				// Convert SourceRef to []string
				var sources []string
				for _, s := range result.Sources {
					if s.Table != "" {
						sources = append(sources, s.Schema+"."+s.Table)
					} else {
						sources = append(sources, s.Schema)
					}
				}

				m := &model.Model{
					Name:            name,
					FilePath:        f,
					Checksum:        checksum,
					Materialization: result.Materialization,
					Layer:           layer,
					DependsOn:       result.Refs,
					Sources:         sources,
					Contract:        result.Contract,
					Tags:            result.Tags,
				}
				models[name] = m
				dag.AddNode(&graph.Node{Name: name, DependsOn: result.Refs, Layer: layer})
			}
		}

		// 2. Determine which models to build
		var plan *planner.BuildPlan

		if buildRelease != "" {
			// Release mode: build only models in the release file
			if buildVersion == "" {
				return fmt.Errorf("--version is required when --release is specified")
			}
			releasePattern := filepath.Join(app.Cfg.Paths.Releases, buildRelease, buildVersion+".yml")
			rel, parseErr := parser.ParseReleaseFile(releasePattern)
			if parseErr != nil {
				return fmt.Errorf("parse release %s@%s: %w", buildRelease, buildVersion, parseErr)
			}
			ui.Info(fmt.Sprintf("Release: %s@%s (%d models)", rel.Name, rel.Version, len(rel.Models)))

			plan, err = planner.Plan(rel, models, dag)
			if err != nil {
				return fmt.Errorf("plan failed: %w", err)
			}
		} else if buildSelect != "" {
			// Select mode: build selected model + transitive deps
			selected := strings.Split(buildSelect, ",")
			for i := range selected {
				selected[i] = strings.TrimSpace(selected[i])
			}
			ui.Info(fmt.Sprintf("Select: building %s + dependencies", buildSelect))

			rel := &release.Release{
				Name:    "select",
				Version: "adhoc",
				Models:  selected,
			}
			plan, err = planner.Plan(rel, models, dag)
			if err != nil {
				return fmt.Errorf("plan failed: %w", err)
			}
		} else {
			// Build-all mode: no --release specified, discover and build everything
			ui.Info(fmt.Sprintf("Build all: %d models discovered", len(models)))

			// Collect all model names
			var allNames []string
			for name := range models {
				allNames = append(allNames, name)
			}

			rel := &release.Release{
				Name:    "all",
				Version: "adhoc",
				Models:  allNames,
			}
			plan, err = planner.Plan(rel, models, dag)
			if err != nil {
				return fmt.Errorf("plan failed: %w", err)
			}
		}

		ui.Info(fmt.Sprintf("Plan: %d steps", len(plan.Steps)))
		for _, step := range plan.Steps {
			fmt.Printf("  %d. %s -> %s (%s)\n", step.Order, step.Model.Name, step.TargetTable, step.TargetDB)
		}

		// 3. Register and execute
		reg := registry.NewRegistry(app.SilverDB(), app.Logger)
		executor := build.NewExecutor(app.SilverDB(), app.FeatureDB(), app.Cfg.DuckDB.Silver, reg, app.Logger)
		if execErr := executor.Execute(ctx, plan); execErr != nil {
			return fmt.Errorf("build failed: %w", execErr)
		}

		ui.Success(fmt.Sprintf("Build complete: %s@%s", plan.ReleaseName, plan.ReleaseVersion))
		return nil
	},
}

func init() {
	buildCmd.Flags().StringVar(&buildRelease, "release", "", "release name (optional — omit to build all models)")
	buildCmd.Flags().StringVar(&buildVersion, "version", "", "release version (required with --release)")
	buildCmd.Flags().StringVar(&buildSelect, "select", "", "build specific model(s) + dependencies (comma-separated)")
	rootCmd.AddCommand(buildCmd)
}
