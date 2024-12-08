create or replace table main.relevant_preds as (
    with latest_train as (
        select max(trained_date) as trained_date
        from main.predictions
    ), preds as (
        from main.predictions
        semi join latest_train using (trained_date)
        select *
    ), with_meta as (
        from preds
        left join fundamentals.meta
            on preds.ticker = upper(meta.ticker)
        select
            preds.*, meta.name, meta.sector, meta.industry, meta.sicSector, meta.location, meta.statementLastUpdated
        where meta.isActive = true
    )
    select * from with_meta
);