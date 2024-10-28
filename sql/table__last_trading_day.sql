create or replace table last_trading_day (
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
