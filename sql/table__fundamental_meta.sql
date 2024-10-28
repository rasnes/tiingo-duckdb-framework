create table if not exists fundamentals.meta (
  permaTicker VARCHAR,
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
  dailyLastUpdated DATE
);
