create or replace view selected_last_trading_day as (
    select
        date,
        close,
        adjClose,
        adjVolume,
        upper(ticker) as ticker,
        splitFactor,
        divCash
    from last_trading_day
    where ticker in (select lower(ticker) from selected_us_tickers)
);
