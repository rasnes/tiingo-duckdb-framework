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

	db, err := extract.NewDuckDB(config, logger)
	if err != nil {
		logger.Error("Error creating DB database: %v", err)
		return
	}
	defer db.Close()

	//// Create a table
	//_, err = db.DB.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)")
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

	err = db.LoadCSV(csvSupportedTickers, "supported_tickers")
	if !errors.Is(nil, err) {
		logger.Error("Error loading supported tickers into DB: %v", err)
		return
	}

	//lastTradingDay, err := httpClient.GetLastTradingDay()
	//if !errors.Is(nil, err) {
	//	logger.Error("Error getting ticker data from last trading day: %v", err)
	//	return
	//}
	//
	//// print the first 10 lines of the CSV
	//fmt.Println(string(lastTradingDay[:300]))
	//
	//err = db.LoadCSV(lastTradingDay, "last_trading_day")
	//if !errors.Is(nil, err) {
	//	logger.Error("Error loading last trading day into DB: %v", err)
	//	return
	//}

	err = db.RunQueryFile("../sql/view__selected_us_tickers.sql")
	if !errors.Is(nil, err) {
		logger.Error("Error creating view selected_us_tickers: %v", err)
		return
	}

	//err = db.RunQueryFile("../sql/table__daily_adjusted.sql")
	//if !errors.Is(nil, err) {
	//	logger.Error("Error creating table daily_adjusted: %v", err)
	//	return
	//}

	err = db.RunQueryFile("../sql/insert__last_trading_day.sql")
	if !errors.Is(nil, err) {
		logger.Error("Error inserting last trading day into daily_adjusted: %v", err)
		return
	}
	// TODO: why doesn't insert happen! No rows are inserted.

	// TODO:
	// - Create logic for reingesting history for a ticker. Could we reuse much from the Python code?
}
