package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	"time"
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
	AppendTable       string   `mapstructure:"append_table"`
	ConnInitFnQueries []string `mapstructure:"conn_init_fn_queries"`
}

type TiingoConfig struct {
	Format    string `mapstructure:"format"`
	StartDate string `mapstructure:"start_date"`
	Columns   string `mapstructure:"columns"`
}

func NewConfig() (*Config, error) {
	env := os.Getenv("APP_ENV")
	if env == "" {
		env = "dev"
	}

	viper.SetConfigName("config.base")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file, %s", err)
	}

	// Load the environment-specific configuration
	viper.SetConfigName(fmt.Sprintf("config.%s", env))
	if err := viper.MergeInConfig(); err != nil {
		log.Fatalf("error reading %s config file, %s", env, err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %v", err)
	}
	config.Env = env

	return &config, nil
}
