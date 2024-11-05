-- TODO: recreate table when
-- switching to paid subscription for fundamentals.
create table if not exists fundamentals.statements (
  date DATE,
  year INTEGER,
  quarter SMALLINT,
  statementType VARCHAR,
  dataCode VARCHAR,
  value DECIMAL,
  primary key (date, year, quarter, statementType, dataCode)
);
