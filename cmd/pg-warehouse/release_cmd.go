package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/parser"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var releaseCmd = &cobra.Command{
	Use:   "release",
	Short: "Manage releases",
}

var releaseListCmd = &cobra.Command{
	Use:   "list",
	Short: "List releases",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		files, _ := filepath.Glob(filepath.Join(app.Cfg.Paths.Releases, "*", "*.yml"))

		if len(files) == 0 {
			ui.Info("No releases found in " + app.Cfg.Paths.Releases)
			return nil
		}

		headers := []string{"RELEASE", "VERSION", "MODELS", "FILE"}
		var rows [][]string
		for _, f := range files {
			rel, parseErr := parser.ParseReleaseFile(f)
			if parseErr != nil {
				ui.Warn(fmt.Sprintf("  skip %s: %v", f, parseErr))
				continue
			}
			rows = append(rows, []string{
				rel.Name,
				rel.Version,
				fmt.Sprintf("%d", len(rel.Models)),
				f,
			})
		}
		ui.Table(headers, rows)
		return nil
	},
}

func init() {
	releaseCmd.AddCommand(releaseListCmd)
	rootCmd.AddCommand(releaseCmd)
}
