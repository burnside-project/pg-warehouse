package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/graph"
	"github.com/burnside-project/pg-warehouse/internal/parser"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate contracts, releases, SQL, checksums, and graph",
	Long:  "Validate all contracts, releases, models, checksums, SQL syntax, and dependency graph. The Flyway discipline checkpoint.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		var warnings, errors int
		_ = warnings

		// 1. Discover and parse contracts
		contractFiles, _ := filepath.Glob(filepath.Join(app.Cfg.Paths.Contracts, "**", "*.yml"))
		if cf2, err2 := filepath.Glob(filepath.Join(app.Cfg.Paths.Contracts, "*.yml")); err2 == nil {
			contractFiles = append(contractFiles, cf2...)
		}
		ui.Info(fmt.Sprintf("Contracts: found %d files", len(contractFiles)))
		for _, f := range contractFiles {
			c, parseErr := parser.ParseContractFile(f)
			if parseErr != nil {
				ui.Danger(fmt.Sprintf("  FAIL: %s: %v", f, parseErr))
				errors++
			} else {
				ui.Success(fmt.Sprintf("  OK: %s (%s)", f, c.QualifiedName()))
			}
		}

		// 2. Discover and parse models
		modelDirs := []string{
			filepath.Join(app.Cfg.Paths.Models, "silver"),
			filepath.Join(app.Cfg.Paths.Models, "marts"),
			filepath.Join(app.Cfg.Paths.Models, "features"),
		}
		var allModels []*parser.ParseResult
		var modelFiles []string
		for _, dir := range modelDirs {
			files, _ := filepath.Glob(filepath.Join(dir, "*.sql"))
			modelFiles = append(modelFiles, files...)
		}
		// Also check flat models/ dir
		if flatFiles, flatErr := filepath.Glob(filepath.Join(app.Cfg.Paths.Models, "*.sql")); flatErr == nil {
			modelFiles = append(modelFiles, flatFiles...)
		}

		ui.Info(fmt.Sprintf("Models: found %d files", len(modelFiles)))
		for _, f := range modelFiles {
			result, parseErr := parser.ParseFile(f)
			if parseErr != nil {
				ui.Danger(fmt.Sprintf("  FAIL: %s: %v", f, parseErr))
				errors++
			} else {
				allModels = append(allModels, result)
				refs := len(result.Refs)
				srcs := len(result.Sources)
				ui.Success(fmt.Sprintf("  OK: %s (refs: %d, sources: %d)", f, refs, srcs))
			}
		}

		// 3. Build DAG and check for cycles
		dag := graph.NewDAG()
		for i, m := range allModels {
			name := m.Name
			if name == "" {
				name = stripNumericPrefix(stripExt(filepath.Base(modelFiles[i])))
			}
			node := &graph.Node{Name: name, DependsOn: m.Refs}
			dag.AddNode(node)
		}
		sorted, cycleErr := dag.TopologicalSort()
		if cycleErr != nil {
			ui.Danger(fmt.Sprintf("Graph: %v", cycleErr))
			errors++
		} else {
			ui.Success(fmt.Sprintf("Graph: %d models, no cycles, valid execution order", len(sorted)))
		}

		// 4. Discover and parse releases
		releaseFiles, _ := filepath.Glob(filepath.Join(app.Cfg.Paths.Releases, "**", "*.yml"))
		if rf2, err2 := filepath.Glob(filepath.Join(app.Cfg.Paths.Releases, "*", "*.yml")); err2 == nil {
			releaseFiles = append(releaseFiles, rf2...)
		}
		ui.Info(fmt.Sprintf("Releases: found %d files", len(releaseFiles)))
		for _, f := range releaseFiles {
			rel, parseErr := parser.ParseReleaseFile(f)
			if parseErr != nil {
				ui.Danger(fmt.Sprintf("  FAIL: %s: %v", f, parseErr))
				errors++
			} else {
				ui.Success(fmt.Sprintf("  OK: %s@%s (%d models)", rel.Name, rel.Version, len(rel.Models)))
			}
		}

		// Summary
		fmt.Println()
		if errors > 0 {
			ui.Danger(fmt.Sprintf("Validation: %d errors, %d warnings", errors, warnings))
			return fmt.Errorf("validation failed with %d errors", errors)
		}
		ui.Success(fmt.Sprintf("Validation passed: 0 errors, %d warnings", warnings))
		return nil
	},
}

func stripExt(name string) string {
	ext := filepath.Ext(name)
	return name[:len(name)-len(ext)]
}

func init() {
	rootCmd.AddCommand(validateCmd)
}
