package pipeline

// import (
// 	"database/sql"
// 	"fmt"
// 	"log/slog"

// 	//"os"

// 	"github.com/rasnes/tiingo-duckdb-framework/EtL/constants"
// 	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
// )

// type DBInterface interface {
// 	LoadCSVWithQuery(csv []byte, queryTemplate string, params map[string]any) (sql.Result, error)
// }

// type ClientInterface interface {
// 	GetMeta(tickers string) ([]byte, error)
// }

// // UpdateMetadata TODO: write docs, rename templateContent variable to something more self-explanatory?
// func UpdateMetadata(db DBInterface, client ClientInterface, logger *slog.Logger, templateContent string) (int, error) {
// 	// Get metadata from Tiingo API
// 	metadata, err := client.GetMeta("")
// 	if err != nil {
// 		return 0, fmt.Errorf("error fetching metadata from Tiingo: %w", err)
// 	}

// 	sqlParams := map[string]any{
// 		"CsvFile": constants.TmpCSVFile,
// 	}

// 	// Load metadata into DuckDB
// 	res, err := db.LoadCSVWithQuery(metadata, templateContent, sqlParams)
// 	if err != nil {
// 		return 0, fmt.Errorf("error loading metadata into DB: %w", err)
// 	}

// 	rowsAffected, err := res.RowsAffected()
// 	if err != nil {
// 		return 0, fmt.Errorf("error getting rows affected: %w", err)
// 	}

// 	logger.Info(fmt.Sprintf("Successfully updated metadata for %d tickers", rowsAffected))

// 	return int(rowsAffected), nil
// }

// type DBGetQueryResults interface {
// 	GetQueryResults(query string) (map[string][]string, error)
// 	LoadCSV(csv []byte, table string, truncate bool) error
// }

// type ClientGetDailyFundamentals interface {
// 	GetDailyFundamentals(ticker string) ([]byte, error)
// }

// // func DailyFundamentals

// func DailyFundamentals(db DBGetQueryResults, client ClientGetDailyFundamentals, logger *slog.Logger, tickers []string) (int, error) {
// 	// TODO: Need support for backfills. Maybe add a fifth parameter? Or, just use the startDate config for it?
// 	// TODO: Add docstring to this function
// 	// TODO: since this function is so sensitive on valid tickers in the API, tickers should be passed as a parameter
// 	// Desired interface: go run main.go fundamentals daily --tickers=msft,hd,cat,aapl for specified tickers
// 	// If not tickers specified, use the selected_fundamentals table
// 	// TODO: error handling of request. It seems like several 4xx errors should be acceptable here, need to figure this out.
// 	// TODO: Prioritization: fix this function just enough to get it to work, then start on the statements function,
// 	// note down all errors received due to not access (e.g. outside of DOW 20, too early startDate etc.), then buy fundamentals 20 years subscription,
// 	// then iron out the kinks and the signature of these functions, AND configure weekly schedules for all these functions.

// 	// query := "select ticker from selected_fundamentals"
// 	// if os.Getenv("APP_ENV") != "prod" {
// 	// 	query += " limit 20"
// 	// }

// 	// res, err := db.GetQueryResults(query)
// 	// if err != nil {
// 	// 	return 0, fmt.Errorf("error getting selected_fundamentals results: %w", err)
// 	// }

// 	// tickers, ok := res["ticker"]
// 	// if !ok {
// 	// 	return 0, fmt.Errorf("ticker key not found in selected_fundamentals results")
// 	// }
// 	// if len(tickers) == 0 {
// 	// 	return 0, fmt.Errorf("no tickers found in selected_fundamentals results")
// 	// }

// 	tickers := []string{"AAPL", "MSFT", "HD", "CAT"}

// 	csvs := make([][]byte, 0)
// 	for _, ticker := range tickers {
// 		daily, err := client.GetDailyFundamentals(ticker)
// 		if err != nil {
// 			return 0, fmt.Errorf("error fetching daily fundamentals for ticker %s: %w", ticker, err)
// 		}
// 		csv, err := load.AddTickerColumn(daily, ticker)
// 		if err != nil {
// 			return 0, fmt.Errorf("error adding ticker column to daily fundamentals for ticker %s: %w", ticker, err)
// 		}

// 		csvs = append(csvs, csv)
// 	}

// 	finalCsv, err := load.ConcatCSVs(csvs)
// 	if err != nil {
// 		return 0, fmt.Errorf("error concatenating CSVs: %w", err)
// 	}

// 	if err := db.LoadCSV(finalCsv, "fundamentals.daily", true); err != nil {
// 		return 0, fmt.Errorf("error loading daily fundamentals to DB: %w", err)
// 	}

// 	return 0, nil
// }
