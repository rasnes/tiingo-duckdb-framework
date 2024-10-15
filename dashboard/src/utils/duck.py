from os import environ
import duckdb
import ibis
import streamlit as st
import altair as alt

motherduck_creds = (
    f"md:{environ['APP_ENV']}?motherduck_token={environ['MOTHERDUCK_TOKEN']}"
)

md_con = duckdb.connect(motherduck_creds)
ibis_con = ibis.duckdb.connect(database=motherduck_creds)


class Daily:
    def __init__(self, table: ibis.Table) -> None:
        self.t = table

    def get_tickers(self) -> ibis.Expr:
        return self.t.select(self.t.ticker).distinct()

    def get_relative(
        self, tickers: list[str], start_date: str, end_date: str
    ) -> ibis.Expr:
        """Calculates the relative price of a stock over a given time period.

        Args:
            tickers: A list of tickers to calculate the relative price for.
            start_date: The start date of the time period.
            end_date: The end date of the time period.

        Returns:
            An ibis expression representing the relative price of the stock.
        """
        # Subquery to perform the aggregation
        t_agg: ibis.Expr = (
            self.t.select("ticker", "date", "adjClose")
            .filter(
                self.t.ticker.isin(ibis.literal(tickers)),
                self.t.date >= ibis.literal(start_date),
                self.t.date <= ibis.literal(end_date),
            )
            .group_by(["ticker", "date"])
            .aggregate(first_adjClose=self.t.adjClose.first())
        )

        # Outer query to calculate the relative value
        return t_agg.mutate(
            relative=t_agg.first_adjClose
            / t_agg.first_adjClose.first().over(
                ibis.window(group_by="ticker", order_by="date")
            )  # type: ignore
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
