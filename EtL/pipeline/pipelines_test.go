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
-P-S,NYSE,Stock,USD,2018-08-22,2023-05-05
000001,SHE,Stock,CNY,2007-01-04,2024-03-01
000007,SHE,Stock,CNY,2007-08-31,2024-03-01
`
			w.Write(createTestZip(csvContent))

		case "/tiingo/daily/prices":
			w.Header().Set("Content-Type", "text/csv")
			csvContent := `ticker,date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor
AAPL,2024-01-01,191.5,192.0,190.5,191.0,1100000,191.5,192.0,190.5,191.0,1100000,0.0,1.0
MSFT,2024-01-02,192.5,193.0,192.0,192.2,1200000,192.5,193.0,192.0,192.2,1200000,0.0,1.0
`
			w.Write([]byte(csvContent))

			// 		case "/tiingo/daily/AAPL/prices":
			// 			w.Header().Set("Content-Type", "text/csv")
			// 			csvContent := `date,adjClose,adjVolume
			// 2024-01-01,191.5,1100000
			// 2024-01-02,192.5,1200000
			// `
			// 			w.Write([]byte(csvContent))

			// 		case "/tiingo/daily/MSFT/prices":
			// 			w.Header().Set("Content-Type", "text/csv")
			// 			csvContent := `date,adjClose,adjVolume
			// 2024-01-01,376.8,850000
			// 2024-01-02,378.2,900000
			// `
			// 			w.Write([]byte(csvContent))

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
		// Handle paths starting with "../sql/"
		if strings.HasPrefix(query, "./sql/") {
			updatedQueries = append(updatedQueries, filepath.Join("..", query))
			continue
		}
		// Handle other paths (if any)
		updatedQueries = append(updatedQueries, query)
	}
	cfg.DuckDB.ConnInitFnQueries = updatedQueries

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

	// Setup config
	cfg := setupTestConfig(t)

	// Create pipeline
	pipeline, err := NewPipeline(cfg, logger)
	assert.NoError(t, err)
	defer pipeline.Close()

	// Override the base URL to use our test server
	pipeline.TiingoClient.BaseURL = server.URL

	// Run the pipeline
	count, err := pipeline.DailyEndOfDay() // Count here is only if backfill happens, not if no backfills are needed.
	assert.NoError(t, err)
	assert.Equal(t, 0, count) // We expect 0 here since there are not backfills required
	// TODO: add case that requires backfill

	// Verify the data in DuckDB
	results, err := pipeline.DuckDB.GetQueryResults("SELECT count(*) as count FROM last_trading_day;")
	fmt.Println("results: ", results)
	assert.NoError(t, err)
	assert.Equal(t, []string{"2"}, results["count"], "Expected 2 rows in last_trading_day table")

	// TODO: add test verifying selected_last_trading_day. The tables to join with should be populated as they should.
	// TODO: add test verifying final table: "SELECT COUNT(*) as count FROM daily_adjusted"
}
