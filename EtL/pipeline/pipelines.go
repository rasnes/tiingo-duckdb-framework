package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/constants"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/utils"
)

type Pipeline struct {
	DuckDB       *load.DuckDB
	TiingoClient *extract.TiingoClient
	Logger       *slog.Logger
	sqlDir       string
	timeProvider utils.TimeProvider
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

	// Determine SQL directory based on working directory
	sqlDir := "sql"
	if _, err := os.Stat(sqlDir); os.IsNotExist(err) {
		// If sql/ doesn't exist in current directory, try parent
		sqlDir = filepath.Join("..", "sql")
		if _, err := os.Stat(sqlDir); os.IsNotExist(err) {
			return nil, fmt.Errorf("cannot find SQL directory in either current or parent directory")
		}
	}

	return &Pipeline{
		DuckDB:       db,
		TiingoClient: httpClient,
		Logger:       logger,
		sqlDir:       sqlDir,
	}, nil
}

func (p *Pipeline) Close() {
	p.DuckDB.Close()
}

func (p *Pipeline) DailyEndOfDay() (int, error) {
	err := p.supportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported tickers: %v", err)
	}

	lastTradingDay, err := p.TiingoClient.GetLastTradingDay()
	if err != nil {
		return 0, fmt.Errorf("error getting ticker data from last trading day: %v", err)
	}

	if err := p.DuckDB.LoadCSV(lastTradingDay, "last_trading_day", false); err != nil {
		return 0, fmt.Errorf("error loading last_trading_day into DB: %v", err)
	}

	if err := p.DuckDB.RunQueryFile(p.getSQLPath("insert__daily_adjusted.sql")); err != nil {
		return 0, fmt.Errorf("error inserting last trading day into daily_adjusted: %v", err)
	}

	res, err := p.DuckDB.GetQueryResultsFromFile(p.getSQLPath("query__selected_backfill.sql"))
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

func (p *Pipeline) selectedFundamentals() ([]string, error) {
	query := "select ticker from fundamentals.selected_fundamentals"
	if os.Getenv("APP_ENV") != "prod" {
		query += " using sample 20"
	}

	res, err := p.DuckDB.GetQueryResults(query)
	if err != nil {
		return nil, fmt.Errorf("error getting fundamentals.selected_fundamentals results: %w", err)
	}

	tickers, ok := res["ticker"]
	if !ok {
		return nil, fmt.Errorf("ticker key not found in fundamentals.selected_fundamentals results")
	}
	if len(tickers) == 0 {
		return nil, fmt.Errorf("no tickers found in fundamentals.selected_fundamentals results")
	}

	return tickers, nil
}

type csvPerTicker func(ticker string) (csv []byte, err error)

func fetchCSVs(tickers []string, fetch csvPerTicker) ([]byte, error) {
	// TODO: This part should probably have more tailored error handling
	// Like some HTTP error codes should be ignored (I might not have access).
	// BUT: it seems the API sends 400 Bad Request with body: None if no access,
	// which is the same as if the request were incorrect. Not optimal.
	csvs := make([][]byte, 0)
	for _, ticker := range tickers {
		daily, err := fetch(ticker)
		if err != nil {
			return nil, fmt.Errorf("error fetching data for ticker %s: %w", ticker, err)
		}
		csv, err := load.AddTickerColumn(daily, ticker)
		if err != nil {
			return nil, fmt.Errorf("error adding ticker column to CSV for ticker %s: %w", ticker, err)
		}

		csvs = append(csvs, csv)
	}

	finalCsv, err := load.ConcatCSVs(csvs)
	if err != nil {
		return nil, fmt.Errorf("error concatenating CSVs: %w", err)
	}

	return finalCsv, nil
}

func (p *Pipeline) DailyFundamentals(tickers []string, half bool) (int, error) {
	if len(tickers) == 0 {
		tickersFromQuery, err := p.selectedFundamentals()
		if err != nil {
			return 0, fmt.Errorf("error getting selected fundamentals: %w", err)
		}

		// Below is a simple workaround for Tiingo's 10k requests per hour.
		// In Github Actions two cron jobs are scheduled one hour apart, to make sure we can fetch data for all tickers.
		// Take the modulo of the current hour to determine which half of the tickers to process.
		// This is a simple way to split the tickers into two halves, each of which could be scheduled on separate clock hours.
		if half {
			tickersFromQuery = utils.HalfOfSlice(tickersFromQuery, time.Now().Hour()%2 == 0)
		}

		tickers = tickersFromQuery
	}

	upperCaseTickers := make([]string, 0)
	for _, ticker := range tickers {
		upperCaseTickers = append(upperCaseTickers, strings.ToUpper(ticker))
	}

	finalCsv, err := fetchCSVs(upperCaseTickers, p.TiingoClient.GetDailyFundamentals)
	if err != nil {
		return 0, fmt.Errorf("error fetching daily fundamentals: %w", err)
	}

	if err := p.DuckDB.LoadCSV(finalCsv, "fundamentals.daily", true); err != nil {
		return 0, fmt.Errorf("error loading daily fundamentals to DB: %w", err)
	}

	return len(tickers), nil
}

func (p *Pipeline) Statements(tickers []string, half bool) (int, error) {
	if len(tickers) == 0 {
		tickersFromQuery, err := p.selectedFundamentals()
		if err != nil {
			return 0, fmt.Errorf("error getting selected fundamentals: %w", err)
		}

		// Below is a simple workaround for Tiingo's 10k requests per hour.
		// In Github Actions two cron jobs are scheduled one hour apart, to make sure we can fetch data for all tickers.
		// Take the modulo of the current hour to determine which half of the tickers to process.
		// This is a simple way to split the tickers into two halves, each of which could be scheduled on separate clock hours.
		if half {
			tickersFromQuery = utils.HalfOfSlice(tickersFromQuery, time.Now().Hour()%2 == 0)
		}

		tickers = tickersFromQuery
	}

	upperCaseTickers := make([]string, 0)
	for _, ticker := range tickers {
		upperCaseTickers = append(upperCaseTickers, strings.ToUpper(ticker))
	}

	finalCsv, err := fetchCSVs(upperCaseTickers, p.TiingoClient.GetStatements)
	if err != nil {
		return 0, fmt.Errorf("error fetching statements: %w", err)
	}

	if err := p.DuckDB.LoadCSV(finalCsv, "fundamentals.statements", true); err != nil {
		return 0, fmt.Errorf("error loading statements to DB: %w", err)
	}

	return len(tickers), nil
}

func (p *Pipeline) UpdateMetadata() (int, error) {
	err := p.supportedTickers()
	if err != nil {
		return 0, fmt.Errorf("error getting supported tickers: %v", err)
	}

	// Get fundamentals metadata for all tickers from Tiingo API
	metadata, err := p.TiingoClient.GetMeta("")
	if err != nil {
		return 0, fmt.Errorf("error fetching metadata from Tiingo: %w", err)
	}

	insertMetaFile := p.getSQLPath("insert__fundamentals_meta.sql")
	templateContent, err := os.ReadFile(insertMetaFile)
	if err != nil {
		return 0, fmt.Errorf("error reading %s file: %w", insertMetaFile, err)
	}

	sqlParams := map[string]any{
		"CsvFile": constants.TmpCSVFile,
	}

	// Load metadata into DuckDB
	res, err := p.DuckDB.LoadCSVWithQuery(metadata, string(templateContent), sqlParams)
	if err != nil {
		return 0, fmt.Errorf("error loading metadata into DB: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return int(rowsAffected), nil
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

// Add this helper method
func (p *Pipeline) getSQLPath(filename string) string {
	return filepath.Join(p.sqlDir, filename)
}

func (p *Pipeline) supportedTickers() error {
	zipSupportedTickers, err := p.TiingoClient.GetSupportedTickers()
	if err != nil {
		return fmt.Errorf("error getting supported_tickers.zip: %v", err)
	}

	csvSupportedTickers, err := extract.UnzipSingleCSV(zipSupportedTickers)
	if err != nil {
		return fmt.Errorf("error unzipping supported_tickers.zip: %v", err)
	}

	if err := p.DuckDB.LoadCSV(csvSupportedTickers, "supported_tickers", false); err != nil {
		return fmt.Errorf("error loading supported_tickers.csv into DB: %v", err)
	}

	return nil
}
