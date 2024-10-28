package cmd

import (
	"fmt"
	"strings"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
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

			db, err := load.NewDuckDB(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating DB database: %w", err)
			}
			defer db.Close()

			httpClient, err := extract.NewTiingoClient(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating HTTP client: %w", err)
			}

			tickers := strings.Split(args[0], ",") // Assuming comma-separated tickers
			nSuccess, err := pipeline.BackfillEndOfDay(tickers, httpClient, log, db)
			if err != nil {
				return fmt.Errorf("error backfilling tickers: %w", err)
			}
			log.Info(fmt.Sprintf("Backfilled %d tickers", nSuccess))
			return nil
		},
	}

	return cmd
}
