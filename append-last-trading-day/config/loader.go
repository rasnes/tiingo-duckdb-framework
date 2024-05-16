package config

import (
	"fmt"
	"github.com/spf13/viper"
	"time"
)

type Config struct {
	Extract struct {
		Backoff struct {
			RetryWaitMin time.Duration `mapstructure:"retry_wait_min"`
			RetryWaitMax time.Duration `mapstructure:"retry_wait_max"`
			RetryMax     int           `mapstructure:"retry_max"`
		}
	}
	DuckDB struct {
		Path string `mapstructure:"path"`
	}
	Tiingo struct {
		Format string `mapstructure:"format"`
	}
}

func NewConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file, %s", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode into struct, %v", err)
	}

	return &config, nil
}
