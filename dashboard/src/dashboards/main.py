import datetime

import streamlit as st
import altair as alt

from utils import duck

st.set_page_config(layout="wide")
st.title("Dashboard")

t = duck.ibis_con.table("daily_adjusted")
st.table(t.limit(5).execute())

daily = duck.Daily(t)


# Cache all_tickers
@st.cache_data
def get_all_tickers():
    return daily.get_tickers().execute()


all_tickers = get_all_tickers()

col1, col2 = st.columns(2)

with col1:
    date_from = st.date_input("From", value=datetime.date(2024, 1, 1))

with col2:
    date_to = st.date_input("To", value=datetime.datetime.now())

# Multiselect widget
selected_tickers = st.multiselect(
    label="Select tickers",
    options=all_tickers.ticker,
    default=["AAPL", "GOOGL"],
    key="ticker_select",
)

# Display the chart using the selected tickers
duck.relative_chart(daily, selected_tickers, date_from, date_to)
