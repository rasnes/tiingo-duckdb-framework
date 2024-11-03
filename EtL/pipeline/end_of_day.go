package pipeline

import (
	"archive/zip"
	"errors"
	"fmt"
	"log/slog"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
)

type Pipeline struct {
	DuckDB       *load.DuckDB
	TiingoClient *extract.TiingoClient
	Logger       *slog.Logger
}

func NewPipeline(config *config.Config, logger *slog.Logger) (*Pipeline, error) {
	db, err := load.NewDuckDB(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error creating DB database: %v", err)
	}

	httpClient, err := extract.NewTiingoClient(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error creating Tiingo HTTP client: %v", err)
	}

	return &Pipeline{
		DuckDB:       db,
		TiingoClient: httpClient,
		Logger:       logger,
	}, nil
}

func (p *Pipeline) Close() {
	p.DuckDB.Close()
}

func (p *Pipeline) DailyEndOfDay() (int, error) {
	defer p.Close()

	zipSupportedTickers, err := p.TiingoClient.GetSupportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported_tickers.csv.zip: %v", err)
	}

	csvSupportedTickers, err := extract.UnzipSingleCSV(zipSupportedTickers)
	if err != nil {
		return 0, fmt.Errorf("error unzipping supported_tickers.csv.zip: %v", err)
	}

	if err := p.DuckDB.LoadCSV(csvSupportedTickers, "supported_tickers", false); err != nil {
		return 0, fmt.Errorf("error loading supported_tickers.csv into DB: %v", err)
	}

	lastTradingDay, err := p.TiingoClient.GetLastTradingDay()
	if err != nil {
		return 0, fmt.Errorf("error getting ticker data from last trading day: %v", err)
	}

	if err := p.DuckDB.LoadCSV(lastTradingDay, "last_trading_day", false); err != nil {
		return 0, fmt.Errorf("error loading last_trading_day into DB: %v", err)
	}

	if err := p.DuckDB.RunQueryFile("../sql/insert__last_trading_day.sql"); err != nil {
		return 0, fmt.Errorf("error inserting last trading day into daily_adjusted: %v", err)
	}

	res, err := p.DuckDB.GetQueryResultsFromFile("../sql/query__selected_backfill.sql")
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

	nTickers, err := p.BackfillEndOfDay(tickers)
	if err != nil {
		return nTickers, fmt.Errorf("error backfilling tickers: %v", err)
	}

	return len(tickers), nil
}

func (p *Pipeline) BackfillEndOfDay(tickers []string) (int, error) {
	var errorList []error
	for i, ticker := range tickers {
		history, err := p.TiingoClient.GetHistory(ticker)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("error fetching history for ticker %s: %w", ticker, err))
			continue
		}

		historyWithTicker, err := load.AddTickerColumn(history, ticker)
		if err != nil {
			errorList = append(errorList, fmt.Errorf("error adding ticker column to history for ticker %s: %w", ticker, err))
			continue
		}

		if err := p.DuckDB.LoadCSV(historyWithTicker, "daily_adjusted", true); err != nil {
			errorList = append(errorList, fmt.Errorf("error loading history to DB for ticker %s: %w", ticker, err))
			continue
		}

		if i > 0 && i%20 == 0 {
			if len(errorList) > 0 {
				p.Logger.Info(fmt.Sprintf("Successfully backfilled %d tickers; failed on %d tickers", i-len(errorList), len(errorList)))
			} else {
				p.Logger.Info(fmt.Sprintf("Successfully backfilled %d tickers", i))
			}
		}
	}

	if len(errorList) > 0 {
		return len(tickers) - len(errorList), errors.Join(errorList...)
	}

	return len(tickers), nil
}
