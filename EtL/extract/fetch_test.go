package extract

import (
	"bytes"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func setup() {
	os.Setenv("TIINGO_TOKEN", "test_token")
}

func teardown() {
	os.Unsetenv("TIINGO_TOKEN")
}

func getTestConfig() *config.Config {
	return &config.Config{
		Tiingo: config.TiingoConfig{
			Format:    "csv",
			StartDate: "2020-01-01",
			Columns:   "open,close",
		},
		Extract: config.ExtractConfig{
			Backoff: config.BackoffConfig{
				RetryWaitMin: 1 * time.Second,
				RetryWaitMax: 2 * time.Second,
				RetryMax:     3,
			},
		},
	}
}

func getTestLogger(buffer *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buffer, nil))
}

func TestNewClient(t *testing.T) {
	setup()
	defer teardown()

	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	client, err := NewClient(cfg, logger)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, "test_token", client.tiingoToken)
	assert.Equal(t, cfg.Tiingo.Format, client.TiingoFormat)
	assert.Equal(t, cfg.Tiingo.StartDate, client.TiingoStartDate)
	assert.Equal(t, cfg.Tiingo.Columns, client.TiingoColumns)
}

func TestNewClient_NoToken(t *testing.T) {
	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	os.Unsetenv("TIINGO_TOKEN")
	client, err := NewClient(cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestClient_FetchData(t *testing.T) {
	setup()
	defer teardown()

	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	client, err := NewClient(cfg, logger)
	assert.NoError(t, err)

	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	client.HTTPClient = retryablehttp.NewClient()
	client.HTTPClient.HTTPClient = server.Client()

	body, err := client.FetchData(server.URL, "test description")
	assert.NoError(t, err)
	assert.Equal(t, []byte("test content"), body)
}

func TestClient_addTiingoConfigToURL(t *testing.T) {
	setup()
	defer teardown()

	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	client, err := NewClient(cfg, logger)
	assert.NoError(t, err)

	rawURL := "https://api.tiingo.com/tiingo/daily/prices"
	expectedURL := "https://api.tiingo.com/tiingo/daily/prices?columns=open%2Cclose&format=csv&startDate=2020-01-01&token=test_token"

	resultURL, err := client.addTiingoConfigToURL(rawURL, true)
	assert.NoError(t, err)
	assert.Equal(t, expectedURL, resultURL)

	// Test without history
	expectedURLWithoutHistory := "https://api.tiingo.com/tiingo/daily/prices?format=csv&token=test_token"
	resultURL, err = client.addTiingoConfigToURL(rawURL, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedURLWithoutHistory, resultURL)
}
