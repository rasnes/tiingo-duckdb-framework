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
	templateContent := `
with relevant_metadata as (
  select *
  from read_csv('{{.CsvFile}}')
)
insert or replace into fundamentals.meta
(
  permaTicker,
  ticker,
  name,
  isActive,
  isADR,
  sector,
  industry,
  sicCode,
  sicSector,
  sicIndustry,
  reportingCurrency,
  location,
  companyWebsite,
  secFilingWebsite,
  statementLastUpdated,
  dailyLastUpdated
)
select
  permaTicker,
  ticker,
  name,
  isActive,
  isADR,
  sector,
  industry,
  sicCode,
  sicSector,
  sicIndustry,
  reportingCurrency,
  location,
  companyWebsite,
  secFilingWebsite,
  statementLastUpdated,
  dailyLastUpdated
from relevant_metadata;`
	return UpdateMetadataWithTemplate(db, client, logger, templateContent)
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
