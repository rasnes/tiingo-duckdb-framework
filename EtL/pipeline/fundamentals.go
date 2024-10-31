package pipeline

import (
	"database/sql"
	"fmt"
	"log/slog"
	//"os"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/constants"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
)

type DBInterface interface {
	LoadCSVWithQuery(csv []byte, queryTemplate string, params map[string]any) (sql.Result, error)
}

type ClientInterface interface {
	GetMeta(tickers string) ([]byte, error)
}

// UpdateMetadata TODO: write docs, rename templateContent variable to something more self-explanatory?
func UpdateMetadata(db DBInterface, client ClientInterface, logger *slog.Logger, templateContent string) (int, error) {
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

type DBGetQueryResults interface {
	GetQueryResults(query string) (map[string][]string, error)
	LoadCSV(csv []byte, table string, truncate bool) error
}

type ClientGetDailyFundamentals interface {
	GetDailyFundamentals(ticker string) ([]byte, error)
}

func DailyFundamentals(db DBGetQueryResults, client ClientGetDailyFundamentals, logger *slog.Logger, templateContent string) (int, error) {
	// TODO: Need support for backfills. Maybre add a fifth parameter?
	// TODO: Add docstring to this function

	// query := "select ticker from selected_fundamentals"
	// if os.Getenv("APP_ENV") != "prod" {
	// 	query += " limit 20"
	// }

	// res, err := db.GetQueryResults(query)
	// if err != nil {
	// 	return 0, fmt.Errorf("error getting selected_fundamentals results: %w", err)
	// }

	// tickers, ok := res["ticker"]
	// if !ok {
	// 	return 0, fmt.Errorf("ticker key not found in selected_fundamentals results")
	// }
	// if len(tickers) == 0 {
	// 	return 0, fmt.Errorf("no tickers found in selected_fundamentals results")
	// }

	tickers := []string{"AAPL", "MSFT", "HD", "CAT"}

	csvs := make([][]byte, 0)
	for _, ticker := range tickers {
		daily, err := client.GetDailyFundamentals(ticker)
		if err != nil {
			return 0, fmt.Errorf("error fetching daily fundamentals for ticker %s: %w", ticker, err)
		}
		csv, err := load.AddTickerColumn(daily, ticker)
		if err != nil {
			return 0, fmt.Errorf("error adding ticker column to daily fundamentals for ticker %s: %w", ticker, err)
		}

		csvs = append(csvs, csv)
	}

	finalCsv, err := load.ConcatCSVs(csvs)
	if err != nil {
		return 0, fmt.Errorf("error concatenating CSVs: %w", err)
	}

	if err := db.LoadCSV(finalCsv, "fundamentals.daily", true); err != nil {
		return 0, fmt.Errorf("error loading daily fundamentals to DB: %w", err)
	}

	return 0, nil
}
