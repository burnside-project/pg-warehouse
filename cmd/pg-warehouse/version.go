package main

import (
	"fmt"

	"github.com/burnside-project/pg-warehouse/pkg/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print pg-warehouse version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pg-warehouse %s\n", version.Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
