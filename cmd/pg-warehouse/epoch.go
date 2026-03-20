package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var epochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Inspect CDC epoch state",
}

var epochListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all epochs with status, LSN, and row count",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		rows, err := app.WH.QueryRows(ctx,
			"SELECT id, status, start_lsn, end_lsn, row_count, started_at, committed_at FROM _epoch.epochs ORDER BY id", 100)
		if err != nil {
			return fmt.Errorf("failed to list epochs: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(rows)
		}

		if len(rows) == 0 {
			ui.Info("No epochs found. Run CDC sync to create epochs.")
			return nil
		}

		headers := []string{"ID", "STATUS", "START LSN", "END LSN", "ROWS", "STARTED", "COMMITTED"}
		tableRows := make([][]string, len(rows))
		for i, row := range rows {
			committed := ""
			if row["committed_at"] != nil {
				committed = fmt.Sprintf("%v", row["committed_at"])
			}
			tableRows[i] = []string{
				fmt.Sprintf("%v", row["id"]),
				fmt.Sprintf("%v", row["status"]),
				fmt.Sprintf("%v", row["start_lsn"]),
				fmt.Sprintf("%v", row["end_lsn"]),
				fmt.Sprintf("%v", row["row_count"]),
				fmt.Sprintf("%v", row["started_at"]),
				committed,
			}
		}
		ui.Table(headers, tableRows)
		return nil
	},
}

var epochStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current open epoch, last merged epoch, and CDC position",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// Get current open epoch
		openRows, err := app.WH.QueryRows(ctx,
			"SELECT id, start_lsn, row_count, started_at FROM _epoch.epochs WHERE status = 'open' ORDER BY id DESC LIMIT 1", 1)
		if err != nil {
			return fmt.Errorf("failed to query open epoch: %w", err)
		}

		// Get last merged epoch
		mergedRows, err := app.WH.QueryRows(ctx,
			"SELECT id, end_lsn, row_count, committed_at FROM _epoch.epochs WHERE status = 'merged' ORDER BY id DESC LIMIT 1", 1)
		if err != nil {
			return fmt.Errorf("failed to query merged epoch: %w", err)
		}

		// Get epoch counts by status
		countRows, err := app.WH.QueryRows(ctx,
			"SELECT status, COUNT(*) as cnt, SUM(row_count) as total_rows FROM _epoch.epochs GROUP BY status ORDER BY status", 10)
		if err != nil {
			return fmt.Errorf("failed to query epoch counts: %w", err)
		}

		if ui.IsJSON() {
			result := map[string]any{
				"open_epoch":   nil,
				"last_merged":  nil,
				"status_counts": countRows,
			}
			if len(openRows) > 0 {
				result["open_epoch"] = openRows[0]
			}
			if len(mergedRows) > 0 {
				result["last_merged"] = mergedRows[0]
			}
			return ui.JSON(result)
		}

		// Display open epoch
		if len(openRows) > 0 {
			row := openRows[0]
			ui.Info(fmt.Sprintf("Current open epoch: %v (LSN: %v, rows: %v, started: %v)",
				row["id"], row["start_lsn"], row["row_count"], row["started_at"]))
		} else {
			ui.Info("No open epoch.")
		}

		// Display last merged
		if len(mergedRows) > 0 {
			row := mergedRows[0]
			ui.Info(fmt.Sprintf("Last merged epoch: %v (LSN: %v, rows: %v, committed: %v)",
				row["id"], row["end_lsn"], row["row_count"], row["committed_at"]))
		} else {
			ui.Info("No merged epochs.")
		}

		// Display status summary
		if len(countRows) > 0 {
			fmt.Println()
			headers := []string{"STATUS", "COUNT", "TOTAL ROWS"}
			rows := make([][]string, len(countRows))
			for i, row := range countRows {
				rows[i] = []string{
					fmt.Sprintf("%v", row["status"]),
					fmt.Sprintf("%v", row["cnt"]),
					formatRowCount(row["total_rows"]),
				}
			}
			ui.Table(headers, rows)
		}

		return nil
	},
}

// formatRowCount safely formats a row count value from a query result.
func formatRowCount(v any) string {
	if v == nil {
		return "0"
	}
	switch val := v.(type) {
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case float64:
		return strconv.FormatInt(int64(val), 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func init() {
	epochCmd.AddCommand(epochListCmd)
	epochCmd.AddCommand(epochStatusCmd)
	rootCmd.AddCommand(epochCmd)
}
