import pytest
import httpx
from src.fetch import Fetch


@pytest.mark.asyncio
async def test_fetch_success(mocker):
    request = httpx.Request(method="GET", url="https://example.com")

    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)
    mock_response = httpx.Response(status_code=200, text="Success", request=request)
    mock_client.get = mocker.AsyncMock(return_value=mock_response)

    fetch_instance = Fetch(mock_client, "token", add_ticker_column=False)

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

    fetch_instance = Fetch(mock_client, "token", add_ticker_column=False)

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

    fetch_instance = Fetch(mock_client, "token", add_ticker_column=False)

    url = "https://example.com"
    result = await fetch_instance._fetch(url)

    assert result is None
    assert "HTTP status error for" in caplog.text


def test_compose_url():
    ticker = "AAPL"
    start_date = "2022-01-01"
    end_date = "2022-01-31"
    fetch_instance = Fetch(
        httpx.AsyncClient(), "token", add_ticker_column=False, response_format="csv"
    )

    url = fetch_instance._compose_url(
        ticker, start_date, end_date, columns=["date", "adjClose"]
    )

    expected_url = "https://api.tiingo.com/tiingo/daily/AAPL/prices?startDate=2022-01-01&endDate=2022-01-31&format=csv&columns=date,adjClose&token=token"
    assert url == expected_url
