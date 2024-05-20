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

	err = db.LoadCSV(csvSupportedTickers, "supported_tickers", false)
	if !errors.Is(nil, err) {
		logger.Error("Error loading supported tickers into DB: %v", err)
		return
	}

	// TODO: handle special case with 200 OK and this body: {"detail":"Not found."}
	// Not sure when it occurs, but might be close to current day's market closing time
	// E.g. now it is 22:20 in Oslo, Norway and markets closed at 22:00
	// Investigate if this timing issue is consistent.
	// If so, that's good. Much better than this occurring randomly.
	// Batch jobs should be scheduled to a time when the API is guaranteed to return data.
	// Sound like 05:00 UTC, or something like that, is a good time.
	lastTradingDay, err := httpClient.GetLastTradingDay()
	if !errors.Is(nil, err) {
		logger.Error("Error getting ticker data from last trading day: %v", err)
		return
	}

	fmt.Println("Last trading day:", string(lastTradingDay[:300]))

	err = db.LoadCSV(lastTradingDay, "last_trading_day", false)
	if !errors.Is(nil, err) {
		logger.Error("Error loading last trading day into DB: %v", err)
		return
	}

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

	res, err := db.GetQueryResultsFromFile("../sql/view__selected_backfill.sql")
	if !errors.Is(nil, err) {
		logger.Error("Error getting backfill results: %v", err)
		return
	}

	tickers, ok := res["ticker"]
	if !ok {
		// TODO: I only want an error here if the key does not exist
		logger.Error("Error getting backfill results: no tickers found")
		return
	}
	if len(tickers) == 0 {
		logger.Info("No tickers to backfill. Batch job completed.")
		return
	}

	fmt.Println(tickers)

	for _, ticker := range tickers {
		history, err := httpClient.GetHistory(ticker)
		if !errors.Is(nil, err) {
			logger.Error("Error getting history for ticker %s: %v", ticker, err)
			return
		}

		historyWithTicker, err := transform.AddTickerColumn(history, ticker)

		err = db.LoadCSV(historyWithTicker, "daily_adjusted", true)
		if !errors.Is(nil, err) {
			logger.Error("Error loading history for ticker %s: %v", ticker, err)
			return
		}
		logger.Info("Backfilled ticker %s", ticker)
	}

	logger.Info("Batch job completed. Backfilled %d tickers", len(tickers))
}
