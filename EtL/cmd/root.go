package cmd

import (
	"fmt"
	"os"

	"log/slog"

	"github.com/joho/godotenv"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/config"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/logger"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "etl",
	Short: "etl cli for different etl tasks",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(endOfDayCmd)
	endOfDayCmd.AddCommand(newDailyCmd())
	endOfDayCmd.AddCommand(newBackfillCmd())
}

func isRunningOnGitHubActions() bool {
	return os.Getenv("GITHUB_ACTIONS") == "true"
}

func initializeConfigAndLogger() (*config.Config, *slog.Logger, error) {
	log := logger.NewLogger()
	if !isRunningOnGitHubActions() {
		err := godotenv.Load()
		if err != nil {
			log.Error("Error loading .env file")
			return nil, nil, err
		}
	}

	cfg, err := config.NewConfig()
	if err != nil {
		log.Error(fmt.Sprintf("Error reading config: %v", err))
		return nil, nil, err
	}

	return cfg, log, nil
}
