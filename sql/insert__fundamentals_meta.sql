with relevant_metadata as (
  select *
  from read_csv('{{.CsvFile}}', delim=',', all_varchar=true)
  -- TODO: figure out why this semi join is not working. It works inside Motherduck!
  -- semi join selected_us_tickers
  --  on upper(meta.ticker) = upper(selected_us_tickers.ticker)
)
insert or replace into fundamentals.meta
(
  permaTicker,
  ticker,
  name,
  isActive,
  isADR,
  sector,
  industry,
  sicCode,
  sicSector,
  sicIndustry,
  reportingCurrency,
  location,
  companyWebsite,
  secFilingWebsite,
  statementLastUpdated,
  dailyLastUpdated
)
select
  permaTicker,
  ticker,
  name,
  isActive,
  isADR,
  sector,
  industry,
  sicCode,
  sicSector,
  sicIndustry,
  reportingCurrency,
  location,
  companyWebsite,
  secFilingWebsite,
  statementLastUpdated,
  dailyLastUpdated
from relevant_metadata;
