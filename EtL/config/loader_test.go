package config

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	tests := []struct {
		name     string
		baseYAML string  // Base YAML config
		envYAML  string  // Environment-specific YAML (optional)
		env      string  // Environment variable value
		want     *Config // Expected Config
		wantErr  bool    // Expecting an error?
	}{
		{
			name: "Successful Load with Default Env",
			baseYAML: `
extract:
  backoff:
    retry_wait_min: 1s
    retry_wait_max: 30s
    retry_max: 5
duckdb:
  path: "test.db"
tiingo:
  eod:
    format: csv
    start_date: "1995-01-01"
    columns: "foo,bar"
  fundamentals:
    daily:
      format: csv
`,
			env: "bar",
			want: &Config{
				Env: "bar",
				Extract: ExtractConfig{
					Backoff: BackoffConfig{
						RetryWaitMin: time.Second,
						RetryWaitMax: 30 * time.Second,
						RetryMax:     5,
					},
				},
				DuckDB: DuckDBConfig{
					Path:              "test.db",
					ConnInitFnQueries: nil,
				},
				Tiingo: TiingoConfig{
					Eod: TiingoAPIConfig{
						Format:    "csv",
						StartDate: "1995-01-01",
						Columns:   "foo,bar",
					},
					Fundamentals: FundamentalsConfig{
						Daily: TiingoAPIConfig{
							Format: "csv",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Successful Load with Environment Override",
			baseYAML: `
duckdb:
  conn_init_fn_queries:
    - "../sql/db__stage.sql"
tiingo:
  eod:
    columns: "date,close"
`,
			envYAML: `
duckdb:
  conn_init_fn_queries:
    - "../sql/db__dev.sql"
tiingo:
  eod:
    format: csv
    columns: "date,open,high,low,close"
`,
			env: "foo",
			want: &Config{
				Env: "foo",
				DuckDB: DuckDBConfig{
					ConnInitFnQueries: []string{"../sql/db__dev.sql"}, // Overridden query
				},
				Tiingo: TiingoConfig{
					Eod: TiingoAPIConfig{
						Format:  "csv",                      // Added format
						Columns: "date,open,high,low,close", // Overridden columns
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset Viper for each test
			viper.Reset()

			// Create a reader for the base YAML
			baseConfigReader := strings.NewReader(tt.baseYAML)
			var envConfigReader io.Reader
			if tt.envYAML != "" {
				envConfigReader = strings.NewReader(tt.envYAML)
			}

			// Call NewConfig with the base config reader
			got, err := NewConfig(baseConfigReader, envConfigReader, tt.env)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got, "Config structs don't match")
		})
	}
}
