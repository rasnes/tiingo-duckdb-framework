package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/config"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/logger"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/pipeline"
)

func main() {
	logger := logger.NewLogger()

	if err := godotenv.Load(); err != nil {
		logger.Error("Error loading .env file")
	}
	config, err := config.NewConfig()
	if err != nil {
		logger.Error(fmt.Sprintf("Error reading config: %v", err))
		return
	}

	nTickers, err := pipeline.EndOfDay(config, logger)
	if err != nil {
		if nTickers > 0 {
			logger.Error(fmt.Sprintf("Error running pipeline: %v. Backfilled %d tickers", err, nTickers))
		} else {
			logger.Error(fmt.Sprintf("Error running pipeline: %v", err))
		}
		return
	}
	logger.Info(fmt.Sprintf("Batch job completed without errors. Backfilled %d tickers", nTickers))
}
