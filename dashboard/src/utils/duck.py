import duckdb
import ibis
from ibis.expr.types import Table
import streamlit as st
import altair as alt

motherduck_creds = (
    f"md:{st.secrets['APP_ENV']}?motherduck_token={st.secrets['MOTHERDUCK_TOKEN']}"
)

md_con = duckdb.connect(motherduck_creds)
ibis_con = ibis.duckdb.connect(database=motherduck_creds)


class Daily:
    def __init__(self, table: Table) -> None:
        self.t: Table = table

    def get_all_tickers(self) -> Table:
        return self.t.select(self.t.ticker).distinct()

    def date_selection(self, tickers, start_date, end_date) -> Table:
        return self.t.filter(
            self.t.ticker.isin(tickers),
            self.t.date >= ibis.literal(start_date),
            self.t.date <= ibis.literal(end_date),
        )

    def get_relative(self, tickers: list[str], start_date: str, end_date: str) -> Table:
        """Calculates the relative price of a stock over a given time period.

        Args:
            tickers: A list of tickers to calculate the relative price for.
            start_date: The start date of the time period.
            end_date: The end date of the time period.

        Returns:
            An ibis expression representing the relative price of the stock.
        """
        # TODO: maybe implement in Polars instead of Ibis?
        # Subquery to perform the aggregation
        t_agg: Table = (
            self.date_selection(tickers, start_date, end_date)
            .group_by(["ticker", "date"])
            .aggregate(first_adjClose=self.t.adjClose.first())
        )

        # Outer query to calculate the relative value
        return t_agg.mutate(
            relative=t_agg.first_adjClose
            / t_agg.first_adjClose.first().over(  # type: ignore
                ibis.window(group_by="ticker", order_by="date")
            )
        )


def relative_chart(daily: Daily, selected_tickers, date_from, date_to) -> None:
    c = (
        alt.Chart(
            daily.get_relative(selected_tickers, str(date_from), str(date_to)).execute()
        )
        .mark_line()
        .encode(x="date", y="relative", color="ticker")
    )
    st.altair_chart(c, use_container_width=True)
