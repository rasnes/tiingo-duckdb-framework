create or replace view selected_us_tickers as (
  with max_end_date as (
    select max(endDate) as maxEndDate
    from supported_tickers
  ), with_duplicates as (
    select supported_tickers.*
    from supported_tickers join max_end_date
      on 1=1
    where
      exchange in ('NYSE', 'NASDAQ', 'NYSE MKT', 'NYSE ARCA', 'AMEX')
      and startDate is not null
      and (
        assetType = 'Stock'
        -- only select ETFs that still exist
        or (assetType = 'ETF' and endDate = max_end_date.maxEndDate)
      )
  )
  select *
  from with_duplicates
  qualify row_number() over (partition by ticker order by endDate desc) = 1
)
