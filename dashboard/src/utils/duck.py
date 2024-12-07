import duckdb
import ibis
from ibis.expr.types import Table
import streamlit as st
import altair as alt
import polars as pl
import pandas as pd
import numpy as np

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


relations = {
    "preds_rel": """
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
""",
}

class Preds:
    def __init__(self, conn: duckdb.DuckDBPyConnection, rel: duckdb.DuckDBPyRelation) -> None:
        self.conn = conn
        self.rel = rel
        self.df: pl.DataFrame
        self.forecasts: pl.DataFrame
    
    def get_all_tickers(self) -> list[str]:
        rel = self.rel
        return [row[0] for row in self.conn.sql("select distinct ticker from rel").fetchall()]
    
    def get_df(self, tickers: list[str]) -> None:
        
        self.df = self.rel.filter(f"ticker in {tickers}").pl()
        
        # Calculate and print DataFrame size
        size_bytes = self.df.estimated_size()
        size_mb = size_bytes / (1024 * 1024)
        print(f"DataFrame size: {size_mb:.2f} MB")
        print(self.df.describe())
    
    def get_forecasts(self):
        self.forecasts = (
            self.df
            .with_columns([
                # Extract number of months
                pl.col("pred_col").str.extract(r"(\d+)m").cast(pl.Int64).alias("months")
            ])
            .with_columns([
                # Use the months column to calculate forecast date
                (pl.col("trained_date") + pl.duration(days=pl.col("months") * 30)).alias("forecast_date")
            ])
            .select("ticker", "trained_date", "forecast_date", "pred_col", "predicted_value", "predicted_std",
                    "name", "sector", "industry", "sicSector", "location")
            .unique()
        )

    def plot_preds(self, container_width=True, random_days_range=20) -> None:
        """Generate a plot showing forecasted returns with confidence intervals.
        
        Args:
            container_width: Whether to use container width in streamlit (default: True)
            random_days_range: Range in days for jittering the dates (default: 20)
        """
        # Convert to pandas and filter if needed
        df = self.forecasts.to_pandas()
            
        # Add jittered dates
        df['jittered_date'] = df['forecast_date'] + pd.to_timedelta(
            np.random.uniform(-random_days_range, random_days_range, len(df)), 
            unit='D'
        )
        
        # Add 2σ confidence interval
        df['predicted_std_2'] = df['predicted_std'] * 2

        # Display info
        st.write(f"Showing predictions for {len(df['ticker'].unique())} tickers: {', '.join(df['ticker'].unique())}")
        
        # Base configuration for both charts
        base = alt.Chart(df)
        
        # Create error bars chart for 2σ (thinner)
        error_bars_2std = base.mark_errorbar(color='white', opacity=0.3).encode(
            x=alt.X('jittered_date:T', 
                    title='Forecast Date',
                    axis=alt.Axis(format='%Y')),
            y=alt.Y('predicted_value', 
                    scale=alt.Scale(zero=False, domain=[0, 2]),
                    title='Predicted Index-relative Return'),
            yError='predicted_std_2',
            color=alt.Color('ticker:N', legend=alt.Legend(title="Ticker"))
        )

        # Create error bars chart for 1σ (thicker)
        error_bars_1std = base.mark_errorbar(color='white', size=5).encode(
            x=alt.X('jittered_date:T'),
            y=alt.Y('predicted_value'),
            yError='predicted_std',
            color=alt.Color('ticker:N')
        )

        # Create points for the mean values
        points = base.mark_point(
            filled=True,
            size=100
        ).encode(
            x='jittered_date:T',
            y='predicted_value',
            color=alt.Color('ticker:N'),
            tooltip=['ticker', 'pred_col', 'predicted_value', 'predicted_std', 
                    alt.Tooltip('predicted_std_2', title='2σ')]
        )

        # Add a vertical line for trained_date
        trained_date_line = base.mark_rule(
            color='white',
            strokeDash=[2, 2]
        ).encode(
            x='trained_date:T',
            tooltip=['trained_date']
        )
        
        # Add horizontal line at y=1
        baseline = base.mark_rule(
            color='yellow',
            opacity=0.5,
            strokeWidth=0.5
        ).encode(
            y=alt.datum(1)  # Fixed y value at 1
        )
        
        # Combine the charts and display
        st.altair_chart(
            (error_bars_2std + error_bars_1std + points + trained_date_line + baseline).properties(
                width=800,
                height=400,
                title="Forecasted Index-relative Returns with 1σ and 2σ Confidence Intervals"
            ).configure_axis(
                labelColor='white',
                titleColor='white',
                gridColor='#444'
            ).configure_title(
                color='white'
            ).configure_legend(
                labelColor='white',
                titleColor='white'
            ),
            use_container_width=True
        )
