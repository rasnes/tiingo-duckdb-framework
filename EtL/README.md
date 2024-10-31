# EtL

Daily batch job running Tuesday-Saturday to get last trading day's end-of-day prices
and appends to the Motherduck table(s).

## Why `EtL` as the name of this directory/package?

I prefer to load and transform data with the ELT (Extract, Load, Transform) framework. However, there is often a need for a small transformation between the Extract and the Load step, like cleaning up types filtering out some of the data. This part I like to call the small `t`, i.e. we end up with `EtLT`. For the main Transformations, see the sibling directory `transformations`.

## TODO

- Fix failed tests.
- Fix double logging for `go run main.go fundamentals metadata`
- Create Taskfile
  - For running linter, tests etc. on `main` PRs
- Add docstrings to all functions and methods
  - Remove redundant explanatory strings by GhatGPT


### Maybe

- fetch.GetLastTradingDay should handle response edge cases. Maybe an exponential backoff or
  sleep if unfamiliar response occurs?
  // TODO: handle special case with 200 OK and this body: {"detail":"Not found."}
  // Not sure when it occurs, but might be close to current day's market closing time
  // E.g. now it is 22:20 in Oslo, Norway and markets closed at 22:00
  // UPDATE: at 22:40, the API returned data again. My guess is that this is consistent.
  // Investigate if this timing issue is consistent.
  // If so, that's good. Much better than this occurring randomly.
  // Batch jobs should be scheduled to a time when the API is guaranteed to return data.
  // Sound like 05:00 UTC, or something like that, is a good time.
- Add backoff functionality for `md:` connections with DuckDB, in case of network failures?
  - Or make a decorator that wraps backoff on the entire pipeline? I kind of like that pattern.
- Handle duplicates in insert__last_trading_day.
  - {"time":"2024-05-20T22:45:03.880005+02:00","level":"ERROR","msg":"Error inserting last trading day into daily_adjusted: %v","!BADKEY":"failed to execute query: Invalid Input Error: ON CONFLICT DO UPDATE can not update the same row twice in the same command. Ensure that no rows proposed for insertion within the same command have duplicate constrained values"}
  - It was a little suspicious that I got almost the exact same stocks for backfill on May 20 as May 19.
    Does it mean that `adj`usting takes several days? Or that the API is slow to update? Or an inaccuracy?
    UPDATE: ran ingest again the next morning, now the list of stocks to backfill had changed significantly. Seems like
    the API is available again before all data is correct. Even more reason to wait a bit with making requests to that API.


## Extract

Will perform two `GET` requests for data:

1. https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip to get an updated
overview of tickers available from Tiingo.
2. https://api.tiingo.com/tiingo/daily/prices to get end-of-day prices for all tickers

## Transform

1. Transforms `supported_tickers.csv` to the selected list of tickers of interest. Via the
`view__selected_us_tickers.sql` transform.
2. Semi join results form API request to https://api.tiingo.com/tiingo/daily/prices with the
`selected_us_tickers` VIEW. This filtering makes sure we're not ingesting unneeded data to
the Motherduck table.

TODO: should the `failed_tickers.csv` be used any way? Currently, I think not, as it will complicate
things and adding

## Load

Ingest the results from Transform.2 above into the Motherduck table.

Use the DuckDB Appender API and log `warning` if primary key already exist (which means
that the ticker-date combination already exists in the database). This strategy assumes that
all data in the Motherduck table is _correct_, which I think is fair; end-of-day prices for yesterday
should never need to be corrected (on Tiingo's end).

Edge case: if `splitFactor` != 1 or `divCash` > 0 for a selected ticker, a reingest of the entire history
for that ticker should be performed (instead of appending it). This is to ensure we get the latest adjusted
prices. For this operation the `INSERT OR REPLACE INTO tbl` strategy will be used for that ticker (which
enables overwriting rows even if there is a violation of a primary key constraint).
