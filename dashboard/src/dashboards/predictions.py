import streamlit as st
import polars as pl
import altair as alt
import numpy as np
import pandas as pd

from utils import duck

# TODO: add shap values somehow. As a pivot table? Or multiple tables next to eadch other?

md_preds = duck.md_con.sql(duck.relations["preds_rel"])
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