create or replace macro sma(col, partition_col, date_col, periods) as
case
    when row_number() over (partition by partition_col order by date_col) > periods
    then avg(col) over (partition by partition_col order by date_col rows between periods preceding and current row)
    else null
end;

-- Updated macro with exchange logic built in
create or replace macro index_volatility(
    price,
    qqq_price,
    spy_price,
    exchange_val,
    partition_col,
    date_col,
    periods
) as
case
    when exchange_val = 'NASDAQ'
        then case
            when row_number() over (partition by partition_col order by date_col) > periods
            then stddev(price/qqq_price) over (partition by partition_col order by date_col rows between periods preceding and current row)
            else null
        end
    else
        case
            when row_number() over (partition by partition_col order by date_col) > periods
            then stddev(price/spy_price) over (partition by partition_col order by date_col rows between periods preceding and current row)
            else null
        end
end;

create or replace macro future_return(price_col, partition_col, date_col, periods) as
    lead(price_col, periods) over (partition by partition_col order by date_col) / nullif(price_col, 0);

-- Macro for excess returns calculation
create or replace macro excess_return(stock_return, qqq_return, spy_return, exchange_val) as
case
    when exchange_val = 'NASDAQ' and qqq_return > 0 and stock_return > 0
        then ln(nullif(stock_return / qqq_return, 0))
    when exchange_val != 'NASDAQ' and spy_return > 0 and stock_return > 0
        then ln(nullif(stock_return / spy_return, 0))
    else null
end;

-- Macro for relative SMA development
create or replace macro relative_sma_development(
    stock_sma,
    qqq_sma,
    spy_sma,
    exchange_val,
    partition_col,
    date_col
) as
case
    when exchange_val = 'NASDAQ'
        then nullif(
            (stock_sma / lag(stock_sma, 1) over (partition by partition_col order by date_col)) /
            (qqq_sma / lag(qqq_sma, 1) over (order by date_col)),
            0)
    when exchange_val != 'NASDAQ'
        then nullif(
            (stock_sma / lag(stock_sma, 1) over (partition by partition_col order by date_col)) /
            (spy_sma / lag(spy_sma, 1) over (order by date_col)),
            0)
    else null
end;
