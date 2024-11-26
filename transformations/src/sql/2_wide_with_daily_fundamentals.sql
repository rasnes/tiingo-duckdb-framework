create or replace view fundamentals.wide_with_daily_fundamentals as (
    select
        wide_statements.*,
        daily.* exclude (ticker, date)
    from fundamentals.wide_statements
    left join fundamentals.daily
        on wide_statements.ticker = daily.ticker and wide_statements.date = daily.date
)
