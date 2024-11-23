create or replace table main.with_index as (
  with daily_adjusted_sma as (
    from main.daily_adjusted
  select
    *,
    sma(adjClose, ticker, date, 125) as SMA_6m,
    sma(adjClose, ticker, date, 250) as SMA_12m,
    sma(adjClose, ticker, date, 500) as SMA_24m,
    sma(adjClose, ticker, date, 750) as SMA_36m,
  ), indices as (
    from daily_adjusted_sma
    select
      ticker,
      date,
      adjClose,
      SMA_6m,
      SMA_12m,
      SMA_24m,
      SMA_36m,
    where ticker in ('SPY', 'QQQ')
  ), pivoted as (
    pivot indices on ticker
    using first(adjClose) as adjClose,
          first(SMA_6m) as SMA_6m,
          first(SMA_12m) as SMA_12m,
          first(SMA_24m) as SMA_24m,
          first(SMA_36m) as SMA_36m
  ), daily_subset as (
    select
      da.*,
      sut.exchange,
    from daily_adjusted_sma as da
    join main.selected_us_tickers as sut
      using (ticker)
    where
      sut.assetType = 'Stock'
      and
        sut.exchange = 'NYSE'
        or (sut.exchange = 'NASDAQ' and da.date >= '2002-10-01')
    -- Option 2: consider skipping tech stocks prior to 2001 crash, as this period might be
    -- so-so training data anyways
    -- UPDATE: leaning towards skipping all Nasdaq stocks prior October 2002
    --  Pros: simple
    --  Cons: arguably a cherrypicking strategy, making the trained model less robust.
    --        it takes away ~7% of the training data (but it might not be optimal training data)
  ), joined as (
    select *
    from pivoted
    join daily_subset as da
      on pivoted.date = da.date
  ), joined_with_volatility as (
    select *,
      index_volatility(adjClose, QQQ_adjClose, SPY_adjClose, exchange, ticker, date, 125) as volatility_6m,
      index_volatility(adjClose, QQQ_adjClose, SPY_adjClose, exchange, ticker, date, 250) as volatility_12m,
      index_volatility(adjClose, QQQ_adjClose, SPY_adjClose, exchange, ticker, date, 500) as volatility_24m,
      index_volatility(adjClose, QQQ_adjClose, SPY_adjClose, exchange, ticker, date, 750) as volatility_36m,
    from joined
  )
  select * exclude (date_1)
  from joined_with_volatility
  -- Sample one day per quarter for each ticker
  -- 16th of the month is proposed since it will be the first
  -- safe_release_date after fiscal months that end on the 31st.
  -- Picking the months that are closest to _after_ safe_release_date.
  where
    day(date) >= 17
    and month(date) in (2, 5, 8, 11)
  qualify row_number() over (
    partition by ticker, year(date), quarter(date) order by date
  ) = 1
)

