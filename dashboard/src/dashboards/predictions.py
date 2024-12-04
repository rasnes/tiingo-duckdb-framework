import streamlit as st
import polars as pl
import altair as alt
import numpy as np
import pandas as pd

from utils import duck


# TODO: wrap plots ++ in functions, and add the plot to main.py as well.
# TODO: add shap values somehow. As a pivot table? Or multiple tables next to eadch other?


preds_rel = """
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
"""

md_preds = duck.md_con.sql(preds_rel)
preds = duck.Preds(duck.md_con, md_preds)

# Cache all_tickers
@st.cache_data
def get_all_tickers():
    return preds.get_all_tickers()

all_tickers = get_all_tickers()

selected_tickers = st.multiselect(
    label="Select tickers",
    options=all_tickers,
    default=["AAPL", "GOOGL"],
    key="ticker_select",
)

if not selected_tickers:
    st.warning("Please select at least one ticker.")
    st.stop()

# Get data for selected tickers
preds.get_df(selected_tickers)
preds.get_forecasts()

preds.plot_preds()

# Show the raw data below
st.dataframe(preds.forecasts)