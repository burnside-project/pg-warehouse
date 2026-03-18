package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/burnside-project/pg-warehouse/internal/adapters/postgres"
	"github.com/burnside-project/pg-warehouse/internal/services"
	"github.com/spf13/cobra"
)

var cdcCmd = &cobra.Command{
	Use:   "cdc",
	Short: "Manage PostgreSQL Change Data Capture",
}

var cdcSetupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create publication and replication slot",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewApp(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		pgSource, err := app.NewPostgresSource()
		if err != nil {
			return err
		}
		defer func() { _ = pgSource.Close() }()

		cdcAdapter := postgres.NewCDCAdapter(app.Cfg.Postgres.URL, pgSource.Pool())
		defer func() { _ = cdcAdapter.Close() }()

		svc := services.NewCDCService(cdcAdapter, app.WH, app.State, pgSource, app.Logger)
		if err := svc.Setup(ctx, app.Cfg.CDC); err != nil {
			return err
		}

		fmt.Println("CDC setup complete")
		fmt.Printf("  publication: %s\n", app.Cfg.CDC.PublicationName)
		fmt.Printf("  slot:        %s\n", app.Cfg.CDC.SlotName)
		fmt.Printf("  tables:      %v\n", app.Cfg.CDC.Tables)
		return nil
	},
}

var cdcTeardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Drop publication and replication slot",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewApp(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		pgSource, err := app.NewPostgresSource()
		if err != nil {
			return err
		}
		defer func() { _ = pgSource.Close() }()

		cdcAdapter := postgres.NewCDCAdapter(app.Cfg.Postgres.URL, pgSource.Pool())
		defer func() { _ = cdcAdapter.Close() }()

		svc := services.NewCDCService(cdcAdapter, app.WH, app.State, pgSource, app.Logger)
		if err := svc.Teardown(ctx, app.Cfg.CDC); err != nil {
			return err
		}

		fmt.Println("CDC teardown complete")
		return nil
	},
}

var cdcFromLSN string

var cdcStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start CDC streaming (Ctrl+C to stop gracefully)",
	Long: `Start CDC streaming from PostgreSQL to DuckDB.

Use --from-lsn to skip the initial snapshot and start streaming from a known LSN.
This is useful when you have pre-seeded DuckDB using a fast bulk copy (pg_dump,
COPY TO CSV, or DuckDB postgres_scan) and want CDC to catch up from that point.

Example workflow:
  1. pg-warehouse cdc setup
  2. psql -c "SELECT pg_current_wal_lsn();"     → 72/6DB940E0
  3. Bulk load tables into DuckDB (COPY, pg_dump, postgres_scan)
  4. pg-warehouse cdc start --from-lsn 72/6DB940E0`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Handle graceful shutdown
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			fmt.Println("\nShutting down CDC gracefully...")
			cancel()
		}()

		app, err := NewApp(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		pgSource, err := app.NewPostgresSource()
		if err != nil {
			return err
		}
		defer func() { _ = pgSource.Close() }()

		cdcAdapter := postgres.NewCDCAdapter(app.Cfg.Postgres.URL, pgSource.Pool())
		defer func() { _ = cdcAdapter.Close() }()

		svc := services.NewCDCService(cdcAdapter, app.WH, app.State, pgSource, app.Logger)

		fmt.Printf("Starting CDC streaming (slot=%s, publication=%s)\n", app.Cfg.CDC.SlotName, app.Cfg.CDC.PublicationName)
		if cdcFromLSN != "" {
			fmt.Printf("Fast-seed mode: skipping snapshot, starting from LSN %s\n", cdcFromLSN)
		}
		fmt.Println("Press Ctrl+C to stop")

		err = svc.Start(ctx, app.Cfg.CDC, app.Cfg.Sync.Tables, cdcFromLSN)
		if err != nil && ctx.Err() != nil {
			// Context cancelled = graceful shutdown
			fmt.Println("CDC stopped gracefully")
			return nil
		}
		return err
	},
}

var cdcStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show replication status",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		app, err := NewApp(ctx, cfgFile)
		if err != nil {
			return err
		}
		defer app.Close()

		pgSource, err := app.NewPostgresSource()
		if err != nil {
			return err
		}
		defer func() { _ = pgSource.Close() }()

		cdcAdapter := postgres.NewCDCAdapter(app.Cfg.Postgres.URL, pgSource.Pool())
		defer func() { _ = cdcAdapter.Close() }()

		svc := services.NewCDCService(cdcAdapter, app.WH, app.State, pgSource, app.Logger)

		status, states, err := svc.Status(ctx, app.Cfg.CDC)
		if err != nil {
			return err
		}

		fmt.Printf("Replication Slot: %s\n", status.SlotName)
		fmt.Printf("  Plugin:        %s\n", status.Plugin)
		fmt.Printf("  Active:        %v\n", status.Active)
		fmt.Printf("  Confirmed LSN: %s\n", status.ConfirmedLSN)
		fmt.Printf("  Current LSN:   %s\n", status.CurrentLSN)
		fmt.Printf("  Lag:           %d bytes\n", status.LagBytes)

		if len(states) > 0 {
			fmt.Println("\nTable States:")
			for _, s := range states {
				fmt.Printf("  %s: status=%s confirmed_lsn=%s\n", s.TableName, s.Status, s.ConfirmedLSN)
			}
		}

		return nil
	},
}

func init() {
	cdcStartCmd.Flags().StringVar(&cdcFromLSN, "from-lsn", "", "skip initial snapshot and start streaming from this LSN (for fast-seed workflows)")
	cdcCmd.AddCommand(cdcSetupCmd)
	cdcCmd.AddCommand(cdcTeardownCmd)
	cdcCmd.AddCommand(cdcStartCmd)
	cdcCmd.AddCommand(cdcStatusCmd)
	rootCmd.AddCommand(cdcCmd)
}
