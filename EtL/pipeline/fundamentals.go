package pipeline

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/constants"
)

type DBInterface interface {
	LoadCSVWithQuery(csv []byte, queryTemplate string, params map[string]any) (sql.Result, error)
}

type ClientInterface interface {
	GetMeta(tickers string) ([]byte, error)
}


func UpdateMetadata(db DBInterface, client ClientInterface, logger *slog.Logger) (int, error) {
	// Get metadata from Tiingo API
	metadata, err := client.GetMeta("")
	if err != nil {
		return 0, fmt.Errorf("error fetching metadata from Tiingo: %w", err)
	}

	// Load metadata using SQL template
	res, err := db.LoadCSVWithQuery(metadata, "../sql/insert__fundamentals_meta.sql", nil)
	if err != nil {
		return 0, fmt.Errorf("error loading metadata: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	logger.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))
	return int(rowsAffected), nil
}

func UpdateMetadataWithTemplate(db DBInterface, client ClientInterface, logger *slog.Logger, templateContent string) (int, error) {
	// Get metadata from Tiingo API
	metadata, err := client.GetMeta("")
	if err != nil {
		return 0, fmt.Errorf("error fetching metadata from Tiingo: %w", err)
	}

	sqlParams := map[string]any{
		"CsvFile": constants.TmpCSVFile,
	}

	// Load metadata into DuckDB
	res, err := db.LoadCSVWithQuery(metadata, templateContent, sqlParams)
	if err != nil {
		return 0, fmt.Errorf("error loading metadata into DB: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	logger.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))

	return int(rowsAffected), nil
}
