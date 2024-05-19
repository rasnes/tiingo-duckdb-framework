with last_trading_day_uppercase_ticker as (
    select
        upper(ticker) as ticker,
        * exclude (ticker)
    from last_trading_day
)
insert or replace into daily_adjusted (date, close, adjClose, adjVolume, ticker)
select date, close, adjClose, adjVolume, ticker
from last_trading_day_uppercase_ticker
where ticker in (select ticker from selected_us_tickers);
