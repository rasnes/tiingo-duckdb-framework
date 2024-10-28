package pipeline

import (
	"errors"
	"fmt"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
	"log/slog"
)

func DailyEndOfDay(config *config.Config, logger *slog.Logger) (int, error) {
	db, err := load.NewDuckDB(config, logger)
	if err != nil {
		return 0, fmt.Errorf("error creating DB database: %v", err)
	}
	defer db.Close()

	httpClient, err := extract.NewTiingoClient(config, logger)
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

	if err := db.LoadCSV(csvSupportedTickers, "supported_tickers", false); err != nil {
		return 0, fmt.Errorf("error loading supported_tickers.csv into DB: %v", err)
	}

	lastTradingDay, err := httpClient.GetLastTradingDay()
	if err != nil {
		return 0, fmt.Errorf("error getting ticker data from last trading day: %v", err)
	}

	if err := db.LoadCSV(lastTradingDay, "last_trading_day", false); err != nil {
		return 0, fmt.Errorf("error loading last_trading_day into DB: %v", err)
	}

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

	nTickers, err := BackfillEndOfDay(tickers, httpClient, logger, db)
	if err != nil {
		return nTickers, fmt.Errorf("error backfilling tickers: %v", err)
	}

	return len(tickers), nil
}

// historyFetcher defines the interface for fetching historical data.
type historyFetcher interface {
	GetHistory(ticker string) ([]byte, error)
}

// csvLoader defines the interface for loading CSV data.
type csvLoader interface {
	LoadCSV(csv []byte, table string, insert bool) error
}

// BackfillEndOfDay fetches historical EoD prices for a list of tickers and loads them into the database
func BackfillEndOfDay(tickers []string, client historyFetcher, logger *slog.Logger, db csvLoader) (int, error) {
	var errorList []error
	for i, ticker := range tickers {
		history, err := client.GetHistory(ticker)
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
