from os import environ
import duckdb
import ibis
import streamlit as st

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
