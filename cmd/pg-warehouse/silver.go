package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var (
	silverLabel   string
	silverVersion int
)

var silverCmd = &cobra.Command{
	Use:   "silver",
	Short: "Manage silver versioned schemas",
}

var silverCreateVersionCmd = &cobra.Command{
	Use:   "create-version",
	Short: "Create a new versioned schema in silver.duckdb",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		svc := services.NewSilverService(app.SilverDB(), app.Logger)
		version, err := svc.CreateVersion(ctx, silverLabel)
		if err != nil {
			return fmt.Errorf("failed to create version: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(map[string]any{
				"version": version,
				"label":   silverLabel,
				"schema":  fmt.Sprintf("v%d", version),
			})
		}

		ui.Success(fmt.Sprintf("Created silver version %d (schema: v%d, label: %s)", version, version, silverLabel))
		return nil
	},
}

var silverPromoteCmd = &cobra.Command{
	Use:   "promote",
	Short: "Promote a versioned schema to current",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		svc := services.NewSilverService(app.SilverDB(), app.Logger)
		if err := svc.Promote(ctx, silverVersion); err != nil {
			return fmt.Errorf("failed to promote version: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(map[string]any{
				"version":  silverVersion,
				"promoted": true,
			})
		}

		ui.Success(fmt.Sprintf("Promoted silver version %d to current", silverVersion))
		return nil
	},
}

var silverListVersionsCmd = &cobra.Command{
	Use:   "list-versions",
	Short: "List all silver versioned schemas",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		svc := services.NewSilverService(app.SilverDB(), app.Logger)
		versions, err := svc.ListVersions(ctx)
		if err != nil {
			return fmt.Errorf("failed to list versions: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(versions)
		}

		if len(versions) == 0 {
			ui.Info("No silver versions found.")
			return nil
		}

		headers := []string{"VERSION", "LABEL", "STATUS"}
		rows := make([][]string, len(versions))
		for i, v := range versions {
			rows[i] = []string{
				strconv.Itoa(v.Version),
				v.Label,
				v.Status,
			}
		}
		ui.Table(headers, rows)
		return nil
	},
}

var silverDropVersionCmd = &cobra.Command{
	Use:   "drop-version",
	Short: "Drop an archived silver version schema",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		svc := services.NewSilverService(app.SilverDB(), app.Logger)
		if err := svc.DropVersion(ctx, silverVersion); err != nil {
			return fmt.Errorf("failed to drop version: %w", err)
		}

		if ui.IsJSON() {
			return ui.JSON(map[string]any{
				"version": silverVersion,
				"dropped": true,
			})
		}

		ui.Success(fmt.Sprintf("Dropped silver version %d", silverVersion))
		return nil
	},
}

func init() {
	silverCreateVersionCmd.Flags().StringVar(&silverLabel, "label", "", "label for the new version")
	_ = silverCreateVersionCmd.MarkFlagRequired("label")

	silverPromoteCmd.Flags().IntVar(&silverVersion, "version", 0, "version number to promote")
	_ = silverPromoteCmd.MarkFlagRequired("version")

	silverDropVersionCmd.Flags().IntVar(&silverVersion, "version", 0, "version number to drop")
	_ = silverDropVersionCmd.MarkFlagRequired("version")

	silverCmd.AddCommand(silverCreateVersionCmd)
	silverCmd.AddCommand(silverPromoteCmd)
	silverCmd.AddCommand(silverListVersionsCmd)
	silverCmd.AddCommand(silverDropVersionCmd)
	rootCmd.AddCommand(silverCmd)
}
