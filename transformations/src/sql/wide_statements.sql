create or replace table fundamentals.wide_statements as (
  with quarterly_statements as (
    -- select relevant columns from statements an concat statementType and dataCode
    select
      ticker,
      date as fiscal_date,
      fiscal_date + 45 as safe_release_date, -- 45 days after fiscal date is the SEC deadline
      concat_ws('-', statementType, dataCode) as metrics,
      value
    from fundamentals.statements
    where
      quarter != 0 -- Ignore annual statements
  ), pivoted as (
    -- pivot statements to a wide table
    pivot quarterly_statements on metrics
    using sum(value) -- Sum should be okay since there are no duplicates
  ), pivoted_with_meta as (
    select *
    from pivoted
    left join fundamentals.meta as meta
      on pivoted.ticker = upper(meta.ticker)
  ), joined as (
    select *
    from main.with_index as da
    asof inner join pivoted_with_meta as pivoted -- inner join due to not interested in tickers without fundamentals
      on da.ticker = pivoted.ticker and da.date >= pivoted.safe_release_date
  )
  select * exclude(
    close,
    fiscal_date,
    safe_release_date, -- OBS: taking this out might make it harder debug if there could be lookahead bias.
    ticker_1,
    ticker_1_1,
    permaTicker,
    sicCode,
    companyWebsite,
    secFilingWebsite,
    statementLastUpdated,
    dailyLastUpdated,
    isActive,
  )
  from joined
  -- Sample one day per month for each ticker
  -- 16th of the month is proposed since it will be the first
  -- safe_release_date after fiscal months that end on the 31st.
  where day(date) >= 17
  qualify row_number() over (
    partition by ticker, year(date), month(date) order by date
  ) = 1
)


-- TODO: daily_adjusted enriched: add SMA
