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
			Eod: config.TiingoAPIConfig{
				Format:    "csv",
				StartDate: "2020-01-01",
				Columns:   "open,close",
			},
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

	client, err := NewTiingoClient(cfg, logger)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, "test_token", client.tiingoToken)
	assert.Equal(t, cfg.Tiingo.Eod.Format, client.TiingoConfig.Eod.Format)
	assert.Equal(t, cfg.Tiingo.Eod.StartDate, client.TiingoConfig.Eod.StartDate)
	assert.Equal(t, cfg.Tiingo.Eod.Columns, client.TiingoConfig.Eod.Columns)
}

func TestNewClient_NoToken(t *testing.T) {
	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	os.Unsetenv("TIINGO_TOKEN")
	client, err := NewTiingoClient(cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestClient_FetchData(t *testing.T) {
	setup()
	defer teardown()

	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	client, err := NewTiingoClient(cfg, logger)
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

func TestParseTodayString(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantError   bool
		errorString string
	}{
		{
			name:      "basic today",
			input:     "today",
			wantError: false,
		},
		{
			name:      "yesterday",
			input:     "today-24h",
			wantError: false,
		},
		{
			name:      "week ago",
			input:     "today-168h",
			wantError: false,
		},
		{
			name:        "invalid format",
			input:       "yesterday",
			wantError:   true,
			errorString: "invalid today string format: yesterday",
		},
		{
			name:        "invalid prefix",
			input:       "tomorrow-24h",
			wantError:   true,
			errorString: "string must start with 'today': tomorrow-24h",
		},
		{
			name:        "invalid duration",
			input:       "today-invalid",
			wantError:   true,
			errorString: "failed to parse duration:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTodayString(tt.input)
			
			if tt.wantError {
				assert.Error(t, err)
				if tt.errorString != "" {
					assert.Contains(t, err.Error(), tt.errorString)
				}
				return
			}
			
			assert.NoError(t, err)
			
			// For successful cases, verify the date format
			_, err = time.Parse("2006-01-02", result)
			assert.NoError(t, err, "Result should be in YYYY-MM-DD format")
			
			// For "today" case, verify it matches today's date
			if tt.input == "today" {
				expected := time.Now().Format("2006-01-02")
				assert.Equal(t, expected, result)
			}
		})
	}
}

func TestClient_addTiingoConfigToURL(t *testing.T) {
	setup()
	defer teardown()

	logger := getTestLogger(&bytes.Buffer{})
	cfg := getTestConfig()

	client, err := NewTiingoClient(cfg, logger)
	assert.NoError(t, err)

	rawURL := "https://api.tiingo.com/tiingo/daily/prices"
	expectedURL := "https://api.tiingo.com/tiingo/daily/prices?columns=open%2Cclose&format=csv&startDate=2020-01-01&token=test_token"

	resultURL, err := client.addTiingoConfigToURL(client.TiingoConfig.Eod, rawURL, true)
	assert.NoError(t, err)
	assert.Equal(t, expectedURL, resultURL)

	// Test without history
	expectedURLWithoutHistory := "https://api.tiingo.com/tiingo/daily/prices?columns=open%2Cclose&format=csv&token=test_token"
	resultURL, err = client.addTiingoConfigToURL(client.TiingoConfig.Eod, rawURL, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedURLWithoutHistory, resultURL)
}
