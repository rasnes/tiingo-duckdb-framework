CREATE TABLE daily_adjusted (
    date DATE,
    adjClose DECIMAL,
    adjVolume UBIGINT,
    ticker VARCHAR,
    primary key (ticker, date)
);
