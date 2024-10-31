-- create table if not exists fundamentals.meta (
-- TODO: switch to `create table if not exists` semantics when
-- switching to paid subscription for fundamentals.
create or replace table fundamentals.meta (
  permaTicker VARCHAR primary key,
  ticker VARCHAR,
  name VARCHAR,
  isActive BOOLEAN,
  isADR BOOLEAN,
  sector VARCHAR,
  industry VARCHAR,
  sicCode VARCHAR,
  sicSector VARCHAR,
  sicIndustry VARCHAR,
  reportingCurrency VARCHAR,
  location VARCHAR,
  companyWebsite VARCHAR,
  secFilingWebsite VARCHAR,
  statementLastUpdated DATE,
  dailyLastUpdated DATE,
);
