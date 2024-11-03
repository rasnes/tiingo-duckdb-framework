-- TODO: double check that this ingest write truncates existing data.
create table if not exists last_trading_day (
  ticker VARCHAR,
  date DATE,
  close DECIMAL,
  high DECIMAL,
  low DECIMAL,
  open DECIMAL,
  volume UBIGINT,
  adjClose DECIMAL,
  adjHigh DECIMAL,
  adjLow DECIMAL,
  adjOpen DECIMAL,
  adjVolume UBIGINT,
  divCash DECIMAL,
  splitFactor DECIMAL
);
