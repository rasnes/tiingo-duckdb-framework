create or replace view fundamentals.selected_fundamentals as (
  with available_eod as (
    select *
    from fundamentals.meta
    semi join selected_us_tickers
      on upper(fundamentals.meta.ticker) = upper(selected_us_tickers.ticker)
  ), deduped as (
    select *
    from available_eod
    qualify row_number() over (partition by ticker order by isActive desc, statementLastUpdated desc) = 1
  )
  select * from deduped
  where dailyLastUpdated is not NULL
)
