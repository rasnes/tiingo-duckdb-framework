# init-db

Python classes and functions for initializing a DuckDB/Motherduck database with data from the Tiingo API. Currently, the code only supports the [end-of-day](https://www.tiingo.com/documentation/end-of-day) API, but I'd guess one could reuse most of this code for any other endpoint with smaller modifications.

## TODO

- Write this README for instructions on how to get it working.
- Configure CI/CD with Github Actions, using the task file.

Once the above is completed, start with the Go ingest:

- Use the appender API for the "regular" daily ingest.
  - If the ingest fails due to primary key constraints, assume that the data already is present in the table. I.e. ignore error.
  - If a stock has splitFactor != 1 or divCash > 0: backfill entire history with the `INSERT OR REPLACE INTO tbl` DuckDB API.
    - Investigate how time-consuming this is, try with e.g. Apple.


## TODO longer term

- Add support for bootstrapping a new table with fundamentals. When I have access to that API.
- What to do about news? Check it out. This might be more useful to feed to an LLM than a database?
