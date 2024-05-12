# init-db

Python classes and functions for initializing a DuckDB/Motherduck database with data from the Tiingo API. Currently, the code only supports the [end-of-day](https://www.tiingo.com/documentation/end-of-day) API, but I'd guess one could reuse most of this code for any other endpoint with smaller modifications.

## TODO

- Check if endDate is needed at all in the API requests, maybe they just can be skipped?
  - And possibly also startDate, that it could be set to 1995-01-01 for all tickers?
  - Benefit: making queries and query logic easier.
- Document classes, functions and methods
- Make linting pass, in particular get the typecheck passing.
- Add tests for all end-user functions and methods
- Move polars transformations to dedicated module and add tests.
- Go through all TODOs scattered around in code and notebook.
- Write this README for instructions on how to get it working.
- Configure CI/CD with Github Actions, using the task file.

Once the above is completed, start with the Go ingest.
