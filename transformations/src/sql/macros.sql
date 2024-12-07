create or replace macro sma(col, partition_col, date_col, periods) as
case
    when row_number() over (partition by partition_col order by date_col) > periods
    then avg(col) over (partition by partition_col order by date_col rows between periods preceding and current row)
    else null
end;

-- Updated macro with exchange logic built in
create or replace macro index_volatility(
    price,
    index_price,
    partition_col,
    date_col,
    periods
) as
case
    when row_number() over (partition by partition_col order by date_col) > periods
    then stddev(price/index_price) over (partition by partition_col order by date_col rows between periods preceding and current row)
    else null
end;

create or replace macro future_return(price_col, partition_col, date_col, periods) as
    lead(price_col, periods) over (partition by partition_col order by date_col) / nullif(price_col, 0);

-- Macro for excess returns calculation
create or replace macro excess_return(stock_return, index_return) as
case
    when index_return > 0 and stock_return > 0
        then ln(nullif(stock_return / index_return, 0))
    else null
end;

-- Macro for relative SMA development
create or replace macro relative_sma_development(
    stock_sma,
    index_sma,
    partition_col,
    date_col
) as
nullif(
    (stock_sma / lag(stock_sma, 1) over (partition by partition_col order by date_col)) /
    (index_sma / lag(index_sma, 1) over (order by date_col)),
    0);


create table if not exists main.predictions (
    date DATE,
    ticker VARCHAR,
    feature VARCHAR,
    shap_value FLOAT,
    feature_value VARCHAR,
    bias FLOAT,
    predicted_value_log FLOAT,
    actual_value_log FLOAT,
    predicted_value FLOAT,
    predicted_std FLOAT,
    actual_value FLOAT,
    pred_col VARCHAR,
    trained_at TIMESTAMP,
    trained_date DATE,
    primary key (date, ticker, feature, pred_col, trained_date)
);
