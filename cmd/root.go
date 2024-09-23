package cmd

import (
	"horizon-io/cmd/aggregate"
	"horizon-io/cmd/price"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{}

func init() {
	rootCmd.AddCommand(price.FetchPrices)
	rootCmd.AddCommand(aggregate.Aggregate)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
