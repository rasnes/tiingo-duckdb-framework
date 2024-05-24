-- TODO: maybe include type (stock/etf) and/or stock exchange?
-- NO: include that data as a dimension table, i.e. make a new view/table
create table if not exists daily_adjusted (
  date DATE,
  close DECIMAL,
  adjClose DECIMAL,
  adjVolume UBIGINT,
  ticker VARCHAR,
  primary key (ticker, date)
);
