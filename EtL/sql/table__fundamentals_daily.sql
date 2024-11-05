create table if not exists fundamentals.daily (
  date DATE,
  marketCap DECIMAL,
  enterpriseVal DECIMAL,
  peRatio DECIMAL,
  pbRatio DECIMAL,
  trailingPEG1Y DECIMAL,
  ticker VARCHAR,
  primary key (ticker, date)
)
