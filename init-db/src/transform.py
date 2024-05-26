import duckdb
import pathlib


def create_selected_us_tickers_view(con: duckdb.DuckDBPyConnection):
    """Create a view of selected US tickers, based on the query in sql/selected_us_tickers.sql."""
    us_tickers_query = pathlib.Path("../sql/view__selected_us_tickers.sql").read_text()
    con.sql(us_tickers_query)
