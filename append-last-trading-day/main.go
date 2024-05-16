package main

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/config"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/extract"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/load"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/logger"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/transform"
	"log"
)

func main() {
	fmt.Println("Main application")
	extract.Foo()
	transform.Bar()
	load.Baz()

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	logger := logger.NewLogger()
	config, err := config.NewConfig()
	if err != nil {
		logger.Error("Error reading config: %v", err)
		return
	}

	db, err := extract.NewDuckDB("", logger)
	if err != nil {
		logger.Error("Error creating DuckDB database: %v", err)
		return
	}
	defer db.Close()

	//// Create a table
	//_, err = db.DuckDB.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)")
	//if err != nil {
	//	log.Fatal(err)
	//}

	httpClient, nil := extract.NewClient(config, logger)
	if !errors.Is(nil, err) {
		logger.Error("Error creating HTTP client: %v", err)
		return
	}

	zipSupportedTickers, err := httpClient.GetSupportedTickers()
	if !errors.Is(nil, err) {
		logger.Error("Error getting supported tickers: %v", err)
		return
	}
	csvSupportedTickers, err := extract.UnzipSingleCSV(zipSupportedTickers)
	if !errors.Is(nil, err) {
		logger.Error("Error unzipping supported tickers: %v", err)
		return
	}

	// print the first 10 lines of the CSV
	fmt.Println(string(csvSupportedTickers[:100]))

	lastTradingDay, err := httpClient.GetLastTradingDay()
	if !errors.Is(nil, err) {
		logger.Error("Error getting ticker data from last trading day: %v", err)
		return
	}

	// print the first 10 lines of the CSV
	fmt.Println(string(lastTradingDay[:1000]))

	// TODO:
	// - Refactor the duckdb loader and methods to use the appender API.
	// - Insert the CSV files into the DuckDB database
	// - Create View for selectedUSTickers
	// - Semi join lastTradingDay on selectedUSTickers
	// - Insert the result into the DuckDB database, using the appender API
	//   - Surface error in ingest as error, but handle it by logging WARN and continue
	// - Create logic for reingesting history for a ticker. Could we reuse much from the Python code?
	//
}
