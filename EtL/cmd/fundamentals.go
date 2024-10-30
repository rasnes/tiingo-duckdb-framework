package cmd

import (
	"fmt"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/pipeline"
	"github.com/spf13/cobra"
)

var fundamentalsCmd = &cobra.Command{
	Use:   "fundamentals",
	Short: "Manage fundamentals data operations",
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

			db, err := load.NewDuckDB(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating DB connection: %w", err)
			}
			defer db.Close()

			client, err := extract.NewTiingoClient(cfg, log)
			if err != nil {
				return fmt.Errorf("error creating HTTP client: %w", err)
			}

			rowsAffected, err := pipeline.UpdateMetadata(db, client, log)
			if err != nil {
				return fmt.Errorf("error updating metadata: %w", err)
			}
			return nil
		},
	}
}

func init() {
	fundamentalsCmd.AddCommand(newMetadataCmd())
}
