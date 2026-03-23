package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/burnside-project/pg-warehouse/internal/parser"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var contractsCmd = &cobra.Command{
	Use:   "contracts",
	Short: "Manage data contracts",
}

var contractsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List data contracts",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// Scan contracts directory
		patterns := []string{
			filepath.Join(app.Cfg.Paths.Contracts, "*.yml"),
			filepath.Join(app.Cfg.Paths.Contracts, "*", "*.yml"),
		}
		var files []string
		for _, p := range patterns {
			f, _ := filepath.Glob(p)
			files = append(files, f...)
		}

		if len(files) == 0 {
			ui.Info("No contracts found in " + app.Cfg.Paths.Contracts)
			return nil
		}

		headers := []string{"NAME", "VERSION", "LAYER", "COLUMNS", "FILE"}
		var rows [][]string
		for _, f := range files {
			c, parseErr := parser.ParseContractFile(f)
			if parseErr != nil {
				ui.Warn(fmt.Sprintf("  skip %s: %v", f, parseErr))
				continue
			}
			rows = append(rows, []string{
				c.Name,
				fmt.Sprintf("v%d", c.Version),
				c.Layer,
				fmt.Sprintf("%d", len(c.Columns)),
				f,
			})
		}
		ui.Table(headers, rows)
		return nil
	},
}

func init() {
	contractsCmd.AddCommand(contractsListCmd)
	rootCmd.AddCommand(contractsCmd)
}
