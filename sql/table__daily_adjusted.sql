-- TODO: maybe include type (stock/etf) and/or stock exchange?
create table daily_adjusted (
  date DATE,
  close DECIMAL,
  adjClose DECIMAL,
  adjVolume UBIGINT,
  ticker VARCHAR,
  primary key (ticker, date)
);
