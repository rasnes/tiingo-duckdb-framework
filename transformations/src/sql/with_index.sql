create or replace table main.with_index as (
  with indices as (
    select ticker, date, adjClose
    from main.daily_adjusted
    where ticker in ('SPY', 'QQQ')
  ), pivoted as (
    pivot indices on ticker
    using sum(adjClose)
  ), daily_subset as (
    select da.*, sut.exchange
    from main.daily_adjusted as da
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
  )
  select * exclude (date_1)
  from joined
)

