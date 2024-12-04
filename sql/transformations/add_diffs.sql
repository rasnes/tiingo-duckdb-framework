with add_diff as (
  from
    daily_adjusted
  select
    *,
    adjClose - LAG(adjClose) OVER (PARTITION BY ticker ORDER BY date) AS diff,
    diff/adjClose*100 as diff_percentage,
  order by
    ticker,
    date
)
select * exclude(diff) from add_diff
order by ticker, date desc
