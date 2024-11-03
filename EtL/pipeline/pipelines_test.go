package pipeline

import (
	"archive/zip"
	"bytes"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
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
			csvContent := "ticker,exchange,assetType,priceCurrency,startDate,endDate\n" +
				"AAPL,NASDAQ,Stock,USD,1980-12-12,2024-01-01\n" +
				"MSFT,NASDAQ,Stock,USD,1986-03-13,2024-01-01"
			w.Write(createTestZip(csvContent))

		case "/tiingo/daily/prices":
			w.Header().Set("Content-Type", "text/csv")
			w.Write([]byte("ticker,date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor\n" +
				"aapl,2024-01-01,190.5,191.0,189.0,190.0,1000000,190.5,191.0,189.0,190.0,1000000,0.24,1.0\n" +
				"msft,2024-01-01,375.8,376.0,374.0,375.0,800000,375.8,376.0,374.0,375.0,800000,0.68,1.0"))

		case "/tiingo/daily/AAPL/prices":
			w.Header().Set("Content-Type", "text/csv")
			w.Write([]byte("date,close,adjClose,adjVolume\n" +
				"2024-01-01,190.5,190.5,1000000\n" +
				"2023-12-31,189.7,189.7,900000"))

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

	return cfg
}

func TestPipeline_DailyEndOfDay(t *testing.T) {
	// Setup test server
	server := setupTestServer()
	defer server.Close()

	// Setup environment
	os.Setenv("TIINGO_TOKEN", "test-token")
	defer os.Unsetenv("TIINGO_TOKEN")

	// Setup logger
	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

	// Setup config using the base config file
	cfg := setupTestConfig(t)

	// Override the SQL file paths for testing to use absolute paths
	var updatedQueries []string
	for _, query := range cfg.DuckDB.ConnInitFnQueries {
		// Convert relative paths to absolute using the project root
		if query[0] == '.' {
			absPath := "../" + query
			updatedQueries = append(updatedQueries, absPath)
		} else {
			updatedQueries = append(updatedQueries, query)
		}
	}
	cfg.DuckDB.ConnInitFnQueries = updatedQueries

	// Create pipeline
	pipeline, err := NewPipeline(cfg, logger)
	assert.NoError(t, err)
	defer pipeline.Close()

	// Override the base URL to use our test server
	pipeline.TiingoClient.BaseURL = server.URL

	// Run the pipeline
	count, err := pipeline.DailyEndOfDay()
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // We expect 2 tickers (AAPL, MSFT)

	// Verify the data in DuckDB
	results, err := pipeline.DuckDB.GetQueryResults("SELECT COUNT(*) as count FROM daily_adjusted")
	assert.NoError(t, err)
	assert.Equal(t, []string{"2"}, results["count"], "Expected 2 rows in daily_adjusted table")
}

