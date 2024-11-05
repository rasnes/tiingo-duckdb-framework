package cmd

import (
	"fmt"
	"strings"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/pipeline"
	"github.com/spf13/cobra"
)

var fundamentalsCmd = &cobra.Command{
	Use:   "fundamentals",
	Short: "Manage fundamentals data operations",
}

func newFundamentalsDailyCmd() *cobra.Command {
	var tickers string

	cmd := &cobra.Command{
		Use:   "daily [--tickers TICKER1,TICKER2,...]",
		Short: "Updates daily fundamentals data for selected tickers",
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

			var tickerSlice []string
			if tickers != "" {
				tickerSlice = strings.Split(tickers, ",")
			}

			rowsAffected, err := pipeline.DailyFundamentals(tickerSlice)
			if err != nil {
				return fmt.Errorf("error updating daily fundamentals: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated daily fundamentals for %d tickers", rowsAffected))

			return nil
		},
	}

	cmd.Flags().StringVar(&tickers, "tickers", "", "Comma-separated list of tickers (e.g., AAPL,MSFT,GOOGL)")
	return cmd
}

func newMetadataCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "metadata",
		Short: "Updates fundamentals metadata for all tickers",
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

			rowsAffected, err := pipeline.UpdateMetadata()
			if err != nil {
				return fmt.Errorf("error updating metadata: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))

			return nil
		},
	}
}

func init() {
	fundamentalsCmd.AddCommand(newMetadataCmd())
	fundamentalsCmd.AddCommand(newFundamentalsDailyCmd())
}
