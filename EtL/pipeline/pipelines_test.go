package pipeline

import (
	"archive/zip"
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/stretchr/testify/assert"
)

func setupTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify token is present
		token := r.URL.Query().Get("token")
		if token == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/docs/tiingo/daily/supported_tickers.zip":
			// Create a minimal zip file with supported tickers CSV
			w.Header().Set("Content-Type", "application/zip")
			csvContent := `ticker,exchange,assetType,priceCurrency,startDate,endDate
AAPL,NASDAQ,Stock,USD,2018-08-22,2024-01-01
MSFT,NASDAQ,Stock,USD,1975-08-22,2024-01-01
ENRON,NASDAQ,Stock,USD,1990-08-22,2005-01-01
000001,SHE,Stock,CNY,2007-01-04,2024-01-01
TQQQ,NASDAQ,ETF,USD,2010-02-11,2024-01-01
ETFGONE,NYSE,ETF,USD,2010-02-11,2010-01-01
TSLA,NASDAQ,Stock,USD,2010-06-29,2024-01-01
AMZN,NASDAQ,Stock,USD,1997-05-15,2024-01-01
`
			w.Write(createTestZip(csvContent))

		case "/tiingo/daily/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `ticker,date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor
aapl,2024-01-01,191.5,192.0,190.5,191.0,1100000,191.5,192.0,190.5,191.0,1100000,0.0,1.0
msft,2024-01-01,192.5,193.0,192.0,192.2,1200000,192.5,193.0,192.0,192.2,1200000,0.0,1.0
tsla,2024-01-01,191.5,192.0,190.5,191.0,1100000,191.5,192.0,190.5,191.0,1100000,0.0,0.9
amzn,2024-01-01,192.5,193.0,192.0,192.2,1200000,192.5,193.0,192.0,192.2,1200000,0.1,1.0
`
			w.Write([]byte(csvContent))

		case "/tiingo/daily/AMZN/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,close,adjClose,adjVolume
2024-01-01,191.5,191.5,1100000
2023-01-01,130.5,130.5,1200000
2021-01-01,191.5,191.5,1100000
`
			w.Write([]byte(csvContent))

		case "/tiingo/daily/TSLA/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `date,close,adjClose,adjVolume
2024-01-01,376.8,376.8,850000
2023-01-01,300.2,300.2,900000
2022-01-01,300.2,300.2,900000
`
			w.Write([]byte(csvContent))

		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("Not found"))
		}
	}))
}

func createTestZip(csvContent string) []byte {
	// Create a buffer to write our zip to
	buf := new(bytes.Buffer)

	// Create a new zip archive
	w := zip.NewWriter(buf)

	// Create a new file inside the zip
	f, err := w.Create("supported_tickers.csv")
	if err != nil {
		panic(err)
	}

	// Write the CSV content to the file
	_, err = f.Write([]byte(csvContent))
	if err != nil {
		panic(err)
	}

	// Close the zip writer
	err = w.Close()
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func setupTestConfig(t *testing.T) *config.Config {
	// Read the base config file
	baseConfig, err := os.Open("../config.base.yaml")
	assert.NoError(t, err)
	defer baseConfig.Close()

	// Create new config with base config and no env config
	cfg, err := config.NewConfig(baseConfig, nil, "test")
	assert.NoError(t, err)

	// Override the DuckDB path to use in-memory database
	cfg.DuckDB.Path = ":memory:"

	// Update SQL file paths for test environment
	var updatedQueries []string
	for _, query := range cfg.DuckDB.ConnInitFnQueries {
		// Handle paths starting with "./sql/"
		if strings.HasPrefix(query, "./sql/") {
			updatedQueries = append(updatedQueries, filepath.Join("..", query))
			continue
		}
		// Handle other paths (if any)
		updatedQueries = append(updatedQueries, query)
	}

	// Add all SQL files from the test directory
	testSQLFiles, err := filepath.Glob("../sql/test/*.sql")
	if err != nil {
		t.Fatalf("Failed to glob test SQL files: %v", err)
	}

	// Sort the files to ensure consistent ordering
	sort.Strings(testSQLFiles)

	// Append test SQL files to the queries
	initAndMockQueries := append(updatedQueries, testSQLFiles...)

	cfg.DuckDB.ConnInitFnQueries = initAndMockQueries

	return cfg
}

func TestPipeline_DailyEndOfDay(t *testing.T) {
	// Configure expected variables (see response from test server)
	expectedInitRowsLastTradingDay := 7
	expectedInitRowsDailyAdjusted := 6
	expectedPostRowsLastTradingDay := 4
	expectedPostRowsSelectedLastTradingDay := 4
	expectedPostRowsSelectedUSTickers := 6
	expectedBackfillRows := 4 // 4 in addition to the 2 with same date as in LastTradingDay

	// Setup test server
	server := setupTestServer()
	defer server.Close()

	// Setup environment
	os.Setenv("TIINGO_TOKEN", "test-token")
	defer os.Unsetenv("TIINGO_TOKEN")

	// Setup logger
	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

	// Setup config
	cfg := setupTestConfig(t)

	// Create pipeline
	pipeline, err := NewPipeline(cfg, logger)
	assert.NoError(t, err)
	defer pipeline.Close()

	// Asserting that existing mock data in the database is as expected
	rowsLastTradingDayPre, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM main.last_trading_day;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedInitRowsLastTradingDay)}, rowsLastTradingDayPre["count"], fmt.Sprintf("Expected %d rows in last_trading_day table", expectedInitRowsLastTradingDay))
	rowsDailyAdjustedPre, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM main.daily_adjusted;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedInitRowsDailyAdjusted)}, rowsDailyAdjustedPre["count"], fmt.Sprintf("Expected %d rows in daily_adjusted table", expectedInitRowsDailyAdjusted))

	// Override the base URL to use our test server
	pipeline.TiingoClient.BaseURL = server.URL

	// Run the pipeline
	count, err := pipeline.DailyEndOfDay() // Count here is only if backfill happens, not if no backfills are needed.
	assert.NoError(t, err)
	nBackfills := 2 // We expect 2 backfills since TSLA and AMZN has divCash or splitFactor in non-normal values
	assert.Equal(t, nBackfills, count)

	// Verify the data in DuckDB
	// Verify selected_us_tickers table
	rowsSelectedUSTickers, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM main.selected_us_tickers;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsSelectedUSTickers)}, rowsSelectedUSTickers["count"], fmt.Sprintf("Expected %d rows in selected_us_tickers table", expectedPostRowsSelectedUSTickers))
	// Verify that the last_trading_day table has been overwritten
	rowsLastTradingDayPost, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM last_trading_day;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsLastTradingDay)}, rowsLastTradingDayPost["count"], fmt.Sprintf("Expected %d rows in last_trading_day table", expectedPostRowsLastTradingDay))
	// Verify that selected_last_trading_day view returns the expected number of rows
	rowsSelectedLastTradingDay, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM selected_last_trading_day;")
	assert.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsSelectedLastTradingDay)}, rowsSelectedLastTradingDay["count"], fmt.Sprintf("Expected %d rows in selected_last_trading_day view", expectedPostRowsSelectedLastTradingDay))
	// Verify that the daily_adjusted table has been updated with newly inserted rows
	rowsDailyAdjustedPost, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM daily_adjusted;")
	assert.NoError(t, err)
	expectedPostRowsDailyAdjusted := expectedInitRowsDailyAdjusted + expectedPostRowsSelectedLastTradingDay + expectedBackfillRows
	assert.Equal(t, []string{fmt.Sprintf("%d", expectedPostRowsDailyAdjusted)}, rowsDailyAdjustedPost["count"], fmt.Sprintf("Expected %d rows in daily_adjusted table", expectedPostRowsDailyAdjusted))
}
