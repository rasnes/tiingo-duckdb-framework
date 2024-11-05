package cmd

import (
	"fmt"
	"strings"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/pipeline"
	"github.com/spf13/cobra"
)

func newBackfillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backfill [tickers]",
		Short: "Backfills historical data for specified tickers",
		Args:  cobra.MinimumNArgs(1), // Requires at least one ticker symbol
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, log, err := initializeConfigAndLogger()
			if err != nil {
				return err
			}

			pipeline, err := pipeline.NewPipeline(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating pipeline: %w", err)
			}
			defer pipeline.Close()

			tickers := strings.Split(args[0], ",") // Assuming comma-separated tickers
			nSuccess, err := pipeline.BackfillEndOfDay(tickers)
			if err != nil {
				return fmt.Errorf("error backfilling tickers: %w", err)
			}
			log.Info(fmt.Sprintf("Backfilled %d tickers", nSuccess))
			return nil
		},
	}

	return cmd
}
