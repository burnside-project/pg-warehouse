package main

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-warehouse/internal/registry"
	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var (
	promoteRelease string
	promoteVersion string
	promoteEnv     string
)

var promoteCmd = &cobra.Command{
	Use:   "promote",
	Short: "Promote a release to an environment",
	Long:  "Promote maps a release/build to an environment alias.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		app, err := NewAppMinimal(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		// Promote current.* views (reuse existing silver promote)
		svc := services.NewSilverService(app.SilverDB(), app.Logger)
		if promoteErr := svc.Promote(ctx, 1); promoteErr != nil {
			app.Logger.Warn("view promotion: %v", promoteErr)
		}

		// Record in _meta.promotions
		reg := registry.NewRegistry(app.SilverDB(), app.Logger)
		if regErr := reg.RecordPromotion(ctx, promoteRelease, promoteVersion, promoteEnv, 0, ""); regErr != nil {
			return fmt.Errorf("record promotion: %w", regErr)
		}

		ui.Success(fmt.Sprintf("Promoted %s@%s to %s", promoteRelease, promoteVersion, promoteEnv))
		return nil
	},
}

func init() {
	promoteCmd.Flags().StringVar(&promoteRelease, "release", "", "release name")
	promoteCmd.Flags().StringVar(&promoteVersion, "version", "", "release version")
	promoteCmd.Flags().StringVar(&promoteEnv, "env", "", "target environment")
	rootCmd.AddCommand(promoteCmd)
}
