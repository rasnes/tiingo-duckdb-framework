package cmd

import (
	"fmt"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
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

			// Get metadata from Tiingo API
			metadata, err := client.GetMeta("")
			if err != nil {
				return fmt.Errorf("error fetching metadata from Tiingo: %w", err)
			}

			// Load metadata using SQL template
			res, err := db.LoadCSVWithQuery(metadata, "../sql/insert__fundamentals_meta.sql", nil)
			if err != nil {
				return fmt.Errorf("error loading metadata: %w", err)
			}

			rowsAffected, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("error getting rows affected: %w", err)
			}

			log.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))
			return nil
		},
	}
}

func init() {
	fundamentalsCmd.AddCommand(newMetadataCmd())
}
