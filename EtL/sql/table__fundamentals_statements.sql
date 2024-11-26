create table if not exists fundamentals.statements (
  date DATE,
  year INTEGER,
  quarter SMALLINT,
  statementType VARCHAR,
  dataCode VARCHAR,
  value DECIMAL,
  ticker VARCHAR,
  primary key (date, year, quarter, statementType, dataCode, ticker)
);
