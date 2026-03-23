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

var graphCmd = &cobra.Command{
	Use:   "graph",
	Short: "Show the model dependency DAG",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

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
				dag.AddNode(&graph.Node{Name: name, DependsOn: result.Refs, Layer: layer})
			}
		}

		sorted, sortErr := dag.TopologicalSort()
		if sortErr != nil {
			return fmt.Errorf("graph error: %w", sortErr)
		}

		ui.Info(fmt.Sprintf("Model DAG (%d nodes)", len(sorted)))
		for i, node := range sorted {
			deps := ""
			if len(node.DependsOn) > 0 {
				deps = fmt.Sprintf(" <- %v", node.DependsOn)
			}
			fmt.Printf("  %d. %s [%s]%s\n", i+1, node.Name, node.Layer, deps)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(graphCmd)
}
