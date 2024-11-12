create or replace table main.with_index as (
  with indices as (
      select ticker, date, adjClose
      from main.daily_adjusted
      where ticker in ('SPY', 'QQQ')
  ), pivoted as (
      pivot indices on ticker
      using sum(adjClose)
  ), relevant_tickers as (
      select *
      from main.selected_us_tickers
      where
          exchange in ('NYSE', 'NASDAQ')
          and assetType = 'Stock'
  ), daily_subset as (
      select *
      from main.daily_adjusted
      semi join relevant_tickers using (ticker)
  ), joined as (
      -- Use SPY as index prior to QQQ existince (March 1999)
      -- TODO: this needs to be refined, as the 2001 tech bubble burst
      -- might need a tech-specific index to work well.
      -- Option 1: create a index manually, as the (weighted) average of all stocks on Nasdaq
      -- Option 2: consider skipping tech stocks prior to 2001 crash, as this period might be
      -- so-so training data anyways
      select * replace (coalesce(QQQ, SPY) as QQQ)
      from pivoted
      join daily_subset as da
          on pivoted.date = da.date
  )
  select * exclude (date_1)
  from joined
)

