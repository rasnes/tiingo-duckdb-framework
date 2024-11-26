create or replace view fundamentals.wide_with_combined_metrics as (
select *,
    -- 1. Efficiency Ratios
    case
        when incomeStatement_revenue > 0 then
        (incomeStatement_sga + incomeStatement_rnd) / incomeStatement_revenue
        else null
    end as operating_efficiency,

    -- 3. Growth Investment Score
    case
        when incomeStatement_revenue > 0 then
        (cashFlow_capex + incomeStatement_rnd) / incomeStatement_revenue
        else null
    end as growth_investment_intensity,

    -- 5. Asset Utilization
    incomeStatement_revenue / NULLIF(balanceSheet_ppeq + balanceSheet_intangibles, 0)
    as asset_productivity,

    -- 6. Profit Stability
    stddev(overview_grossMargin) over (
        partition by ticker
        order by date
        rows between 4 preceding and current row
    ) as margin_stability,

    -- 7. Working Capital Efficiency
    (balanceSheet_inventory + balanceSheet_acctRec - balanceSheet_acctPay) /
    NULLIF(incomeStatement_revenue, 0) * 365 as working_capital_days,

    -- 9. Enterprise value ratios
    incomeStatement_ebitda / NULLIF(enterpriseVal, 0) as ev_to_ebitda,
    cashFlow_freeCashFlow / NULLIF(enterpriseVal, 0) as ev_to_fcf,
    incomeStatement_revenue / NULLIF(enterpriseVal, 0) as ev_to_sales,

    -- 11. Financial Health Score
    overview_currentRatio / nullif(overview_debtEquity, 0) as financial_health_score,

    from fundamentals.wide_with_daily_fundamentals
)
