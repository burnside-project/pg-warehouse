package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/burnside-project/pg-warehouse/internal/adapters/duckdb"
	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspect local warehouse state",
}

var inspectTablesCmd = &cobra.Command{
	Use:   "tables",
	Short: "List all warehouse tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// In multi-file mode, inspect silver.duckdb (where developer tables live)
		inspector := duckdb.NewInspector(app.SilverDB().DB())
		svc := services.NewInspectService(inspector, app.State)

		tables, err := svc.ListTables(ctx)
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(tables)
		}

		if len(tables) == 0 {
			ui.Info("No tables found in warehouse.")
			return nil
		}

		headers := []string{"SCHEMA", "TABLE", "ROWS"}
		rows := make([][]string, len(tables))
		for i, t := range tables {
			rows[i] = []string{t.Schema, t.Name, strconv.FormatInt(t.RowCount, 10)}
		}
		ui.Table(headers, rows)
		return nil
	},
}

var inspectSchemaCmd = &cobra.Command{
	Use:   "schema [table]",
	Short: "Describe table schema",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// In multi-file mode, inspect silver.duckdb
		inspector := duckdb.NewInspector(app.SilverDB().DB())
		svc := services.NewInspectService(inspector, app.State)

		schema, err := svc.DescribeTable(ctx, args[0])
		if err != nil {
			return fmt.Errorf("failed to describe table: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(schema)
		}

		fmt.Printf("Table: %s.%s\n\n", schema.Schema, schema.Name)
		headers := []string{"COLUMN", "TYPE", "NULLABLE", "POSITION"}
		rows := make([][]string, len(schema.Columns))
		for i, col := range schema.Columns {
			nullable := "NO"
			if col.Nullable {
				nullable = "YES"
			}
			rows[i] = []string{col.Name, col.Type, nullable, strconv.Itoa(col.Position)}
		}
		ui.Table(headers, rows)
		return nil
	},
}

var inspectSyncStateCmd = &cobra.Command{
	Use:   "sync-state",
	Short: "Show sync state for all tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		svc := services.NewInspectService(nil, app.State)

		states, err := svc.GetSyncState(ctx)
		if err != nil {
			return fmt.Errorf("failed to get sync state: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(states)
		}

		if len(states) == 0 {
			ui.Info("No sync state found. Run 'pg-warehouse sync' first.")
			return nil
		}

		headers := []string{"TABLE", "MODE", "STATUS", "ROWS", "LAST SYNC"}
		rows := make([][]string, len(states))
		for i, s := range states {
			lastSync := "never"
			if s.LastSyncAt != nil {
				lastSync = s.LastSyncAt.Format("2006-01-02 15:04:05")
			}
			rows[i] = []string{s.TableName, s.SyncMode, s.LastStatus, strconv.FormatInt(s.RowCount, 10), lastSync}
		}
		ui.Table(headers, rows)
		return nil
	},
}

func init() {
	inspectCmd.AddCommand(inspectTablesCmd)
	inspectCmd.AddCommand(inspectSchemaCmd)
	inspectCmd.AddCommand(inspectSyncStateCmd)
	rootCmd.AddCommand(inspectCmd)
}
