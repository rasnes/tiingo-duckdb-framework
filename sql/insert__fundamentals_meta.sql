with relevant_metadata as (
  select *
  from read_csv('{{.CsvFile}}')
  -- TODO: add semi-join here
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
