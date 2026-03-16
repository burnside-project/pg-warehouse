package main

import (
	"github.com/burnside-project/pg-warehouse/internal/ui"
	"github.com/spf13/cobra"
)

var (
	cfgFile  string
	jsonFlag bool
)

var rootCmd = &cobra.Command{
	Use:   "pg-warehouse",
	Short: "Local PostgreSQL to DuckDB analytics CLI",
	Long: `pg-warehouse mirrors PostgreSQL data into DuckDB, lets you run SQL feature
pipelines, and exports results to Parquet or CSV.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		ui.SetJSON(jsonFlag)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "pg-warehouse.yml", "config file path")
	rootCmd.PersistentFlags().BoolVar(&jsonFlag, "json", false, "output in JSON format")
}
