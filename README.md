# tiingo-duckdb-framework

Downloads financial data from Tiingo to Motherduck and displays them in Observable Framework.

WIP: ETA for alpha completion is fall 2024.

[Tiingo](https://www.tiingo.com/about/pricing) has a restrictive license for its stock data, so there is _no_ data available in this repo
and the Motherduck database in use is for my private usage only. However, the idea is that if you bring your own Tiingo API key to this project,
you could get things up and running yourself pretty quickly. 

> [!NOTE]
> This is a hobby project. My main focus is to get things up and running for myself, not that it works without effort for anybody else.
> But feel free to use as much as you'd like from it.

## TODOs/Roadmap

As this is early stage, tools and approaches might change along the way, but the plan in May 2024 looks something like this:

- [ ] Backfill [Motherduck](https://motherduck.com/) DB with all US stocks daily adjusted as listed in this file: https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
- [ ] Create daily batch job to updated with yesterday's data. Use Go and run job on GitHub Actions. Endpoint: https://api.tiingo.com/tiingo/daily/prices
- [ ] Subscribe to the Tiingo $10/month add-on for fundamentals, run backfill for all available stocks and schedule daily fundamentals ingest (Go+Github Actions).
- [ ] Use [Malloy](https://docs.malloydata.dev/documentation/) for transformations.
- [ ] Create visualizations, tables, dashboards and notebooks in [Observable Framework](https://observablehq.com/framework/).
- [ ] Use [Malloy](https://docs.malloydata.dev/documentation/) for the semantic layer/metrics definitions, which will be used by the Observable Framework front-end.
- [ ] Orchestrate statistical and machine learning models with [dagster](https://dagster.io/) running on Github Actions and save results to Motherduck DB.
