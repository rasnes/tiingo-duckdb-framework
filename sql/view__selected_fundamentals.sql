create or replace view fundamentals.selected_fundamentals as (
  select *
  from fundamentals.meta
  semi join selected_us_tickers
    on upper(fundamentals.meta.ticker) = upper(selected_us_tickers.ticker)
)
