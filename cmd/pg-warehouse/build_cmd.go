package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/build"
	"github.com/burnside-project/pg-warehouse/internal/domain/model"
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
)

var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Run a graph-resolved build for a release",
	Long:  "Build materializes release outputs using DAG-resolved model execution. Flyway-style discipline, dbt-style graph.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// 1. Parse release
		releasePattern := filepath.Join(app.Cfg.Paths.Releases, buildRelease, buildVersion+".yml")
		rel, parseErr := parser.ParseReleaseFile(releasePattern)
		if parseErr != nil {
			return fmt.Errorf("parse release %s@%s: %w", buildRelease, buildVersion, parseErr)
		}
		ui.Info(fmt.Sprintf("Release: %s@%s (%d models)", rel.Name, rel.Version, len(rel.Models)))

		// 2. Discover all models
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

		// 3. Generate build plan
		plan, planErr := planner.Plan(rel, models, dag)
		if planErr != nil {
			return fmt.Errorf("plan failed: %w", planErr)
		}
		ui.Info(fmt.Sprintf("Plan: %d steps", len(plan.Steps)))
		for _, step := range plan.Steps {
			fmt.Printf("  %d. %s -> %s (%s)\n", step.Order, step.Model.Name, step.TargetTable, step.TargetDB)
		}

		// 4. Register and execute
		reg := registry.NewRegistry(app.SilverDB(), app.Logger)
		executor := build.NewExecutor(app.SilverDB(), app.FeatureDB(), reg, app.Logger)
		if execErr := executor.Execute(ctx, plan); execErr != nil {
			return fmt.Errorf("build failed: %w", execErr)
		}

		ui.Success(fmt.Sprintf("Build complete: %s@%s", rel.Name, rel.Version))
		return nil
	},
}

func init() {
	buildCmd.Flags().StringVar(&buildRelease, "release", "", "release name")
	buildCmd.Flags().StringVar(&buildVersion, "version", "", "release version")
	_ = buildCmd.MarkFlagRequired("release")
	_ = buildCmd.MarkFlagRequired("version")
	rootCmd.AddCommand(buildCmd)
}
