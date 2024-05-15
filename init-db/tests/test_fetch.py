import pytest
import httpx
import io
import pathlib
import polars as pl

from src.fetch import Fetch, _save_to_csv_with_ticker_col


def test_save_to_csv_with_ticker_col(mocker):
    # Sample data for testing
    response_text = "column1,column2\nvalue1,value2"
    path = pathlib.Path("/fake/path/to/file.csv")
    ticker = "AAPL"

    # Mock the DataFrame and its methods
    mock_df = pl.DataFrame({"column1": ["value1"], "column2": ["value2"]})

    # Mock the read_csv function
    mock_read_csv = mocker.patch("src.fetch.pl.read_csv", return_value=mock_df)

    # Mock the write_csv method
    mock_write_csv = mocker.patch.object(pl.DataFrame, "write_csv")

    # Mock the with_columns method
    mock_with_columns = mocker.patch.object(
        pl.DataFrame, "with_columns", return_value=mock_df
    )

    # Call the function
    _save_to_csv_with_ticker_col(response_text, path, ticker)

    # Assertions
    mock_read_csv.assert_called_once()
    assert isinstance(mock_read_csv.call_args[0][0], io.StringIO)
    assert mock_read_csv.call_args[0][0].getvalue() == response_text

    mock_with_columns.assert_called_once()
    mock_write_csv.assert_called_once_with(path)

    # Check that the ticker column was added correctly
    args, kwargs = mock_with_columns.call_args
    assert len(args) == 1


def test_compose_url():
    ticker = "AAPL"
    start_date = "2022-01-01"
    fetch_instance = Fetch(httpx.AsyncClient(), "token", response_format="csv")

    url = fetch_instance._compose_url(ticker, start_date, columns=["date", "adjClose"])

    expected_url = "https://api.tiingo.com/tiingo/daily/AAPL/prices?startDate=2022-01-01&format=csv&columns=date,adjClose&token=token"
    assert url == expected_url


@pytest.mark.asyncio
async def test_fetch_success(mocker):
    request = httpx.Request(method="GET", url="https://example.com")

    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)
    mock_response = httpx.Response(status_code=200, text="Success", request=request)
    mock_client.get = mocker.AsyncMock(return_value=mock_response)

    fetch_instance = Fetch(mock_client, "token")

    url = "https://example.com"
    result = await fetch_instance._fetch(url)

    assert result == "Success"
    mock_client.get.assert_awaited_once_with(url)


@pytest.mark.asyncio
async def test_fetch_request_error(mocker, caplog):
    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)
    mock_client.get = mocker.AsyncMock(
        side_effect=httpx.RequestError(
            "error", request=httpx.Request("GET", "https://example.com")
        )
    )

    fetch_instance = Fetch(mock_client, "token")

    url = "https://example.com"
    result = await fetch_instance._fetch(url)

    assert result is None
    assert "An error occurred while requesting" in caplog.text


@pytest.mark.asyncio
async def test_fetch_http_status_error(mocker, caplog):
    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)
    mock_response = httpx.Response(status_code=404)
    mock_client.get = mocker.AsyncMock(
        return_value=mock_response,
        side_effect=httpx.HTTPStatusError(
            "error",
            request=httpx.Request("GET", "https://example.com"),
            response=mock_response,
        ),
    )

    fetch_instance = Fetch(mock_client, "token")

    url = "https://example.com"
    result = await fetch_instance._fetch(url)

    assert result is None
    assert "HTTP status error for" in caplog.text


@pytest.mark.asyncio
async def test_fetch_to_disk(mocker):
    # Sample data for testing
    response_text = "column1,column2\nvalue1,value2"
    ticker = "AAPL"
    url = "https://api.tiingo.com/tiingo/daily/AAPL/prices?startDate=1995-01-01&format=csv&token=fake_token"
    path = pathlib.Path("/fake/path/to/file.csv")

    # Mock the _fetch method
    mock_fetch = mocker.patch("src.fetch.Fetch._fetch", return_value=response_text)

    # Mock the _compose_url method
    mock_compose_url = mocker.patch("src.fetch.Fetch._compose_url", return_value=url)

    # Mock the _compose_path method
    mock_compose_path = mocker.patch("src.fetch.Fetch._compose_path", return_value=path)

    # Mock the _save_to_csv_with_ticker_col function
    mock_save_to_csv = mocker.patch("src.fetch._save_to_csv_with_ticker_col")

    # Mock the _write_failed_ticker method
    mock_write_failed_ticker = mocker.patch("src.fetch.Fetch._write_failed_ticker")

    # Create an instance of Fetch
    client = mocker.Mock()
    fetch_instance = Fetch(client, "fake_token")

    # Call the fetch_to_disk method
    await fetch_instance.fetch_to_disk(ticker)

    # Assertions
    mock_compose_url.assert_called_once_with(ticker, "1995-01-01", None)
    mock_compose_path.assert_called_once_with(ticker, "1995-01-01")
    mock_fetch.assert_called_once_with(url)
    mock_save_to_csv.assert_called_once_with(response_text, path, ticker)
    mock_write_failed_ticker.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_to_disk_no_data(mocker):
    # Sample data for testing
    response_text = ""
    ticker = "AAPL"
    url = "https://api.tiingo.com/tiingo/daily/AAPL/prices?startDate=1995-01-01&format=csv&token=fake_token"
    path = pathlib.Path("/fake/path/to/file.csv")

    # Mock the _fetch method
    mock_fetch = mocker.patch("src.fetch.Fetch._fetch", return_value=response_text)

    # Mock the _compose_url method
    mock_compose_url = mocker.patch("src.fetch.Fetch._compose_url", return_value=url)

    # Mock the _compose_path method
    mock_compose_path = mocker.patch("src.fetch.Fetch._compose_path", return_value=path)

    # Mock the _save_to_csv_with_ticker_col function
    mock_save_to_csv = mocker.patch("src.fetch._save_to_csv_with_ticker_col")

    # Mock the _write_failed_ticker method
    mock_write_failed_ticker = mocker.patch("src.fetch.Fetch._write_failed_ticker")

    # Create an instance of Fetch
    client = mocker.Mock()
    fetch_instance = Fetch(client, "fake_token")

    # Call the fetch_to_disk method
    await fetch_instance.fetch_to_disk(ticker)

    # Assertions
    mock_compose_url.assert_called_once_with(ticker, "1995-01-01", None)
    mock_compose_path.assert_called_once_with(ticker, "1995-01-01")
    mock_fetch.assert_called_once_with(url)
    mock_save_to_csv.assert_not_called()
    mock_write_failed_ticker.assert_called_once_with(ticker)
