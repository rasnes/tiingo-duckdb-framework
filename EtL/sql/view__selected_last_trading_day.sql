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
    semi join selected_us_tickers
        on lower(last_trading_day.ticker) = lower(selected_us_tickers.ticker)
    qualify row_number() over (partition by ticker order by divCash desc) = 1
);
