package pipeline

import (
	"github.com/rasnes/tiingo-duckdb-framework/EtL/extract"
	"github.com/rasnes/tiingo-duckdb-framework/EtL/load"
	"log/slog"
)

func UpdateMetadata(tickers []string, httpClient *extract.TiingoClient, logger *slog.Logger, db *load.DuckDB) {
	// 1. convert tickers to string of comma separated values
	// 2. use the httpClient.GetMeta(tickers) to get the metadata
	// 3. get all tickers in Motherduck table and make a semi-join on the downloaded CSV before ingesting
	// 4 use 'insert or replace' to update the metadata table
	// 4. log the number of rows inserted
}
