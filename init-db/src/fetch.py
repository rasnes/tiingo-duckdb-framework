import io
import zipfile
import httpx
import logging
import datetime
import polars as pl
from asyncio import TaskGroup
import pathlib

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# TODO: add relevant tests for this class


class Fetch:
    def __init__(
        self,
        client: httpx.AsyncClient,
        tiingo_token: str,
        add_ticker_column: bool,
        min_start_date: str = "1995-01-01",
        save_dir: str = "data",
        response_format: str = "csv",
        failed_tickers_file: str = "failed_tickers.csv",
    ):
        self._client = client
        self._save_dir = save_dir
        self._response_format = response_format
        self._tiingo_token = tiingo_token
        self._add_ticker_column = add_ticker_column
        self._min_start_date = min_start_date
        self._failed_tickers_file = pathlib.Path(failed_tickers_file)
        if not self._failed_tickers_file.exists():
            self._failed_tickers_file.touch()
            self._failed_tickers_file.write_text("ticker,date\n")
    # TODO: document this method

    async def fetch_to_disk(
        self,
        ticker: str,
        columns: list[str] | None = None,
    ):
        """Compose the URL and path, then fetch data and save it to disk."""
        url = self._compose_url(ticker, self._min_start_date, columns)
        path = self._compose_path(ticker, self._min_start_date)
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

            if self._add_ticker_column:
                (
                    pl.read_csv(io.StringIO(response_text))
                    .with_columns(pl.lit(ticker).alias("ticker"))
                    .write_csv(path)
                )
            else:
                path.write_text(response_text)
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
        `startDate` and `endDate` columns are expected in the DataFrame.
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
        self, df: pl.DataFrame, columns: list[str], async_batch_size: int = 500
    ):
        """Helper function to fetch all the data in the DataFrame in batches.
        The fetch_supported_tickers method seemed to run into concurrency issues when fetching all the data at once.
        Remember: there is a 10k request limit per hour for the Tiingo API.
        """
        total_rows = df.shape[0]
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

    def _compose_path(
        self, ticker: str, start_date: str
    ) -> pathlib.Path:
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


def get_supported_tickers(
    url: str = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip",
) -> pl.DataFrame:
    """Fetch the list of supported tickers from the Tiingo API."""
    response = httpx.get(url)
    zip_data = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_data, "r") as zip_ref:
        csv_filename = zip_ref.namelist()[0]

        with zip_ref.open(csv_filename) as csv_file:
            csv_data = io.BytesIO(csv_file.read())
            df = pl.read_csv(
                source=csv_data,
                schema={
                    "ticker": pl.String,
                    "exchange": pl.String,
                    "assetType": pl.String,
                    "priceCurrency": pl.String,
                    "startDate": pl.Date,
                    "endDate": pl.Date,
                },
            )

    return df
