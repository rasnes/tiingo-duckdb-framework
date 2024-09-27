import datetime
import duckdb
import pytest
import pathlib
from collections import Counter
from src.transform import create_selected_us_tickers_view


@pytest.fixture
def duckdb_connection():
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def test_create_selected_us_tickers_view(duckdb_connection):
    sql_file_path = pathlib.Path("../sql/view__selected_us_tickers.sql")
    assert sql_file_path.exists(), f"SQL file {sql_file_path} does not exist."

    # Create test data
    duckdb_connection.execute("""
        CREATE TABLE supported_tickers (
            ticker VARCHAR,
            exchange VARCHAR,
            assetType VARCHAR,
            startDate DATE,
            endDate DATE
        );
    """)

    duckdb_connection.execute("""
        INSERT INTO supported_tickers VALUES
        ('AAPL', 'NASDAQ', 'Stock', '1980-12-12', '2023-01-01'),
        ('GOOGL', 'NASDAQ', 'Stock', '2004-08-19', '2023-01-01'),
        ('INVALID', 'NASDAQ', 'Stock', null, null),
        ('INVALID2', 'NASDAQ', 'Stock', null, '2023-01-01'), -- This case has not been observed in the data
        ('SPY', 'AMEX', 'ETF', '1993-01-29', '2022-01-01'),
        ('SPY', 'NYSE MKT', 'ETF', '1993-01-29', '2023-01-01'),
        ('OLD_STOCK', 'NYSE ARCA', 'Stock', '1990-01-01', '2000-01-01'),
        ('OLD_ETF', 'NYSE ARCA', 'ETF', '1990-01-01', '2000-01-01');
    """)

    # Call the function to create the view
    create_selected_us_tickers_view(duckdb_connection)

    # Query the view and verify the results
    result = duckdb_connection.execute("SELECT * FROM selected_us_tickers").fetchall()

    print(Counter(result))

    expected_result = [
        (
            "AAPL",
            "NASDAQ",
            "Stock",
            datetime.date(1980, 12, 12),
            datetime.date(2023, 1, 1),
        ),
        (
            "GOOGL",
            "NASDAQ",
            "Stock",
            datetime.date(2004, 8, 19),
            datetime.date(2023, 1, 1),
        ),
        (
            "SPY",
            "NYSE MKT",
            "ETF",
            datetime.date(1993, 1, 29),
            datetime.date(2023, 1, 1),
        ),
        (
            "OLD_STOCK",
            "NYSE ARCA",
            "Stock",
            datetime.date(1990, 1, 1),
            datetime.date(2000, 1, 1),
        ),
    ]

    # Use Counter in assertion to ignore row ordering
    assert Counter(result) == Counter(expected_result)
