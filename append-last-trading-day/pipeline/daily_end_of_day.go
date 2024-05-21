package pipeline

import (
	"errors"
	"fmt"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/config"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/extract"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/load"
	"log/slog"
)

func EndOfDay(config *config.Config, logger *slog.Logger) (int, error) {
	db, err := load.NewDuckDB(config, logger)
	if err != nil {
		return 0, fmt.Errorf("error creating DB database: %v", err)
	}
	defer db.Close()

	httpClient, err := extract.NewClient(config, logger)
	if err != nil {
		return 0, fmt.Errorf("error creating HTTP client: %v", err)
	}

	zipSupportedTickers, err := httpClient.GetSupportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported_tickers.csv.zip: %v", err)
	}
	csvSupportedTickers, err := extract.UnzipSingleCSV(zipSupportedTickers)
	if err != nil {
		return 0, fmt.Errorf("error unzipping supported_tickers.csv.zip: %v", err)
	}

	// TODO: remove below. print the first 10 lines of the CSV
	fmt.Println(string(csvSupportedTickers[:100]))

	if err := db.LoadCSV(csvSupportedTickers, "supported_tickers", false); err != nil {
		return 0, fmt.Errorf("error loading supported_tickers.csv into DB: %v", err)
	}

	// TODO: handle special case with 200 OK and this body: {"detail":"Not found."}
	// Not sure when it occurs, but might be close to current day's market closing time
	// E.g. now it is 22:20 in Oslo, Norway and markets closed at 22:00
	// UPDATE: at 22:40, the API returned data again. My guess is that this is consistent.
	// Investigate if this timing issue is consistent.
	// If so, that's good. Much better than this occurring randomly.
	// Batch jobs should be scheduled to a time when the API is guaranteed to return data.
	// Sound like 05:00 UTC, or something like that, is a good time.

	//lastTradingDay, err := httpClient.GetLastTradingDay()
	//if err != nil {
	//	return fmt.Errorf("error getting ticker data from last trading day: %v", err)
	//}
	//
	//// TODO: remove below
	//fmt.Println("Last trading day:", string(lastTradingDay[:300]))
	//
	//if err := db.LoadCSV(lastTradingDay, "last_trading_day", false); err != nil {
	//	return fmt.Errorf("error loading last_trading_day into DB: %v", err)
	//}

	if err := db.RunQueryFile("../sql/insert__last_trading_day.sql"); err != nil {
		return 0, fmt.Errorf("error inserting last trading day into daily_adjusted: %v", err)
	}

	res, err := db.GetQueryResultsFromFile("../sql/query__selected_backfill.sql")
	if err != nil {
		return 0, fmt.Errorf("error getting backfill results: %v", err)
	}

	tickers, ok := res["ticker"]
	if !ok {
		return 0, errors.New("ticker key not found in selected_backfill.sql results")
	}
	if len(tickers) == 0 {
		return 0, nil
	}

	// TODO: remove below
	fmt.Println(tickers)

	nTickers, err := backfillTickers(tickers, httpClient, logger, db)
	if err != nil {
		return nTickers, fmt.Errorf("error backfilling tickers: %v", err)
	}

	return len(tickers), nil
}

func backfillTickers(tickers []string, httpClient *extract.Client, logger *slog.Logger, db *load.DuckDB) (int, error) {
	var errorList []error
	for i, ticker := range tickers {
		history, err := httpClient.GetHistory(ticker)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("error fetching history for ticker %s: %w", ticker, err))
			continue
		}

		historyWithTicker, err := load.AddTickerColumn(history, ticker)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("error adding ticker column to history for ticker %s: %w", ticker, err))
			continue
		}

		if err := db.LoadCSV(historyWithTicker, "daily_adjusted", true); err != nil {
			errorList = append(errorList, fmt.Errorf("error loading history to DB for ticker %s: %w", ticker, err))
			continue
		}

		if i > 0 && i%20 == 0 {
			if len(errorList) > 0 {
				nErrors := len(errorList)
				logger.Info(fmt.Sprintf("Successfully backfilled %d tickers; failed on %d tickers", i-nErrors, nErrors))
			} else {
				logger.Info(fmt.Sprintf("Successfully backfilled %d tickers", i))
			}
		}
	}

	if len(errorList) > 0 {
		return len(tickers) - len(errorList), errors.Join(errorList...)
	}

	return len(tickers), nil
}
