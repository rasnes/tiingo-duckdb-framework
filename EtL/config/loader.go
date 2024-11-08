package config

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Extract ExtractConfig
	DuckDB  DuckDBConfig
	Tiingo  TiingoConfig
	Env     string
}

type ExtractConfig struct {
	Backoff BackoffConfig
}

type BackoffConfig struct {
	RetryWaitMin time.Duration `mapstructure:"retry_wait_min"`
	RetryWaitMax time.Duration `mapstructure:"retry_wait_max"`
	RetryMax     int           `mapstructure:"retry_max"`
}

type DuckDBConfig struct {
	Path              string   `mapstructure:"path"`
	ConnInitFnQueries []string `mapstructure:"conn_init_fn_queries"`
}

type TiingoConfig struct {
	Eod          TiingoAPIConfig    `mapstructure:"eod"`
	Fundamentals FundamentalsConfig `mapstructure:"fundamentals"`
}

type FundamentalsConfig struct {
	Daily      TiingoAPIConfig `mapstructure:"daily"`
	Statements TiingoAPIConfig `mapstructure:"statements"`
	Meta       TiingoAPIConfig `mapstructure:"meta"`
}

type TiingoAPIConfig struct {
	Format    string `mapstructure:"format"`
	StartDate string `mapstructure:"start_date"`
	Columns   string `mapstructure:"columns"`
}

// NewConfig loads the configuration from the provided base config reader
// and merges it with the environment-specific configuration.
func NewConfig(baseConfigReader io.Reader, envConfigReader io.Reader, env string) (*Config, error) {
	if env == "" { // Use the provided 'env' or default to "dev"
		env = "dev"
	}

	viper.SetConfigType("yaml")

	// Read the base configuration
	if err := viper.ReadConfig(baseConfigReader); err != nil {
		return nil, fmt.Errorf("error reading base config: %w", err)
	}

	// Merge with environment-specific configuration (only if provided)
	if envConfigReader != nil {
		if err := viper.MergeConfig(envConfigReader); err != nil {
			log.Printf("Error merging environment-specific config: %s", err)
			// Handle the error as needed (log, return error, etc.)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	// Set the environment directly
	config.Env = env

	return &config, nil
}
