import io
import zipfile
import httpx
import logging
import datetime
import tempfile
import polars as pl
import duckdb
from asyncio import TaskGroup
import pathlib

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def _save_to_csv_with_ticker_col(
    response_text: str, path: pathlib.Path, ticker: str
) -> None:
    """Save the fetched data to a CSV file with the ticker as the last column.

    Parameters
    ----------
    response_text : str
        The response text from the API.
    path : pathlib.Path
        The path to save the CSV file.
    ticker : str
        The ticker symbol.
    """
    (
        pl.read_csv(io.StringIO(response_text))
        .with_columns(pl.lit(ticker).alias("ticker"))
        .write_csv(path)
    )


def get_supported_tickers(
    url: str = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip",
    supported_tickers_query: pathlib.Path = pathlib.Path(
        "../sql/table__supported_tickers.sql"
    ),
) -> duckdb.DuckDBPyConnection:
    """Fetch the list of supported tickers from the Tiingo API.

    Parameters
    ----------
    url : str, optional
        The URL to fetch the supported tickers from, by default "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
    supported_tickers_query : pathlib.Path, optional
        The path to the SQL query to create the table for supported tickers, by default pathlib.Path("sql/supported_tickers.sql")

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection to an in-memory DuckDB database with the supported tickers.
    """
    response = httpx.get(url)
    zip_data = io.BytesIO(response.content)

    con = duckdb.connect(":memory:")
    create_table_query = supported_tickers_query.read_text()
    con.execute(create_table_query)

    with zipfile.ZipFile(zip_data, "r") as zip_ref:
        csv_filename = zip_ref.namelist()[0]

        with zip_ref.open(csv_filename) as csv_file:
            with tempfile.NamedTemporaryFile(delete=False, mode="w+b") as temp_file:
                temp_file.write(csv_file.read())
                temp_file_path = temp_file.name

            con.execute(
                f"COPY supported_tickers FROM '{temp_file_path}' (DELIMITER ',')"
            )

    return con


class Fetch:
    """Fetch historical end-of-day prices from the Tiingo API.

    Attributes
    ----------
    _client : httpx.AsyncClient
        httpx.AsyncClient is used for making async requests to the API.
    _tiingo_token : str
        The Tiingo API token.
    _start_date : str, optional
        The start date for fetching data, by default "1995-01-01".
    _save_dir : str, optional
        The directory to save the fetched data, by default "data".
    _response_format : str, optional
        The format of the response, by default "csv".
    _failed_tickers_file : str, optional
        The file to write failed tickers to, by default "failed_tickers.csv".
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        tiingo_token: str,
        start_date: str = "1995-01-01",
        save_dir: str = "data",
        response_format: str = "csv",
        failed_tickers_file: str = "failed_tickers.csv",
    ):
        """Initialize the Fetch object.

        Parameters
        ----------
        client : httpx.AsyncClient
            httpx.AsyncClient is used for making async requests to the API.
        tiingo_token : str
            The Tiingo API token.
        start_date : str, optional
            The start date for fetching data, by default "1995-01-01".
        save_dir : str, optional
            The directory to save the fetched data, by default "data".
        response_format : str, optional
            The format of the response, by default "csv".
        failed_tickers_file : str, optional
            The file to write failed tickers to, by default "failed_tickers.csv".
        """
        self._client = client
        self._save_dir = save_dir
        self._response_format = response_format
        self._tiingo_token = tiingo_token
        self._start_date = start_date
        self._failed_tickers_file = pathlib.Path(failed_tickers_file)
        if not self._failed_tickers_file.exists():
            self._failed_tickers_file.touch()
            self._failed_tickers_file.write_text("ticker,date\n")

    async def fetch_to_disk(
        self,
        ticker: str,
        columns: list[str] | None = None,
    ):
        """Compose the URL and path, then fetch data and save it to disk."""
        url = self._compose_url(ticker, self._start_date, columns)
        path = self._compose_path(ticker, self._start_date)
        response_text = await self._fetch(url)
        if response_text is not None:
            if response_text == "[]" or response_text == "":
                logger.error(f"Invalid response for {ticker}.")
                self._write_failed_ticker(ticker)
                return
            # early return if response_text is only one line, i.e. only column names but no data
            if len(response_text.splitlines()) == 1:
                logger.error(f"No data found for {ticker}.")
                self._write_failed_ticker(ticker)
                return

            _save_to_csv_with_ticker_col(response_text, path, ticker)
            logger.info(f"Saved data for {ticker} to {path}")
        else:
            logger.error(f"response.text for {ticker} is None.")
            self._write_failed_ticker(ticker)

    async def fetch_supported_tickers(
        self,
        df: pl.DataFrame,
        columns: list[str] | None = None,
    ):
        """Fetch historical end-of-day prices for the tickers in the DataFrame and write to disk.

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame containing the tickers. Required columns: "ticker".
        columns : list[str], optional
            The columns to fetch, by default None (which will fetch all columns).
        """
        tasks = []
        async with TaskGroup() as tg:
            for row in df.rows(named=True):
                ticker = row["ticker"]
                tasks.append(
                    tg.create_task(
                        self.fetch_to_disk(
                            ticker=ticker,
                            columns=columns,
                        )
                    )
                )

        for task in tasks:
            task.result()

    async def fetch_all(
        self, df: pl.DataFrame, columns: list[str] | None, async_batch_size: int = 500
    ):
        """Wrapper function around `fetch_supported_tickers` to fetch all the data in the DataFrame
        in batches and write to disk. The fetch_supported_tickers method runs into concurrency issues
        when fetching all the data at once.
        Remember: there is a 10k request limit per hour for the Tiingo API.

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame containing the tickers. Required columns: "ticker".
        columns : list[str]
            The columns to fetch, by default None (which will fetch all columns).
        async_batch_size : int, optional
            The batch size for fetching data asynchronously, by default 500.

        Raises
        ------
        ValueError
            If the number of rows in the DataFrame is greater than 10,000.
        """

        total_rows = df.shape[0]
        if total_rows > 10000:
            raise ValueError(
                "The number of rows in the DataFrame is greater than 10,000, which exceeds the hourly "
                "Tiingo API request limit."
            )

        for start in range(0, total_rows, async_batch_size):
            df_batch = df.slice(start, async_batch_size)
            print(f"Fetching {start} to {start+async_batch_size} of {total_rows}")
            await self.fetch_supported_tickers(
                df=df_batch,
                columns=columns,
            )

    async def _fetch(self, url: str) -> str | None:
        """Fetch data from the given URL."""
        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            logger.error(f"An error occurred while requesting {url}: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error for {url}: {e}")
        except Exception as e:
            logger.exception(f"An unexpected error occurred for {url}: {e}")

    def _compose_path(self, ticker: str, start_date: str) -> pathlib.Path:
        """Compose the path for saving the historical end-of-day prices."""
        filename = f"{ticker}_{start_date}.{self._response_format}"
        return pathlib.Path(f"{self._save_dir}/{filename}")

    def _compose_url(
        self,
        ticker: str,
        start_date: str,
        columns: list[str] | None = None,
    ) -> str:
        """Compose the URL for fetching historical end-of-day prices."""
        base_url = "https://api.tiingo.com/tiingo/daily"
        url = f"{base_url}/{ticker}/prices?startDate={start_date}&format={self._response_format}"
        if columns is not None:
            url += f"&columns={','.join(columns)}"
        return f"{url}&token={self._tiingo_token}"

    def _write_failed_ticker(self, ticker: str):
        """Write the failed ticker to the file."""
        with self._failed_tickers_file.open("a") as f:
            f.write(f"{ticker},{datetime.datetime.today().strftime('%Y-%m-%d')}\n")
