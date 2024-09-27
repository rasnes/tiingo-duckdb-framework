# from dagster_duckdb import DuckDBResource
# from dagster_duckdb_polars import DuckDBPolarsIOManager
from os import environ
import datetime
from dagster import (
    Definitions,
    asset,
    resource,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableSchema,
    define_asset_job,
)
import polars as pl
import duckdb


@resource
def duckdb_resource(_):
    return {
        "database": f"""md:{environ["APP_ENV"]}?motherduck_token={environ["MOTHERDUCK_TOKEN"]}""",
        "schema": "main",
    }


def query_duckdb(sql: str, db_config: dict[str, str]) -> pl.DataFrame:
    con = duckdb.connect(database=db_config["database"], read_only=True)
    df = con.query(sql).pl()
    con.close()
    return df


def write_table_duckdb(
    table_name: str, df: pl.DataFrame, db_config: dict[str, str]
) -> None:
    df.write_database(
        table_name=table_name,
        connection=f"duckdb:///{db_config['database']}",
        if_table_exists="replace",
    )


@asset(required_resource_keys={"duckdb_config"})
def daily_table(context: AssetExecutionContext) -> pl.DataFrame:
    db_config = context.resources.duckdb_config
    return query_duckdb("SELECT * FROM daily_adjusted where ticker = 'AGZD'", db_config)


@asset
def single_day(daily_table: pl.DataFrame) -> pl.LazyFrame:
    return daily_table.filter(pl.col("date") == pl.datetime(2021, 1, 4)).lazy()


@asset
def first_row(daily_table: pl.DataFrame) -> pl.LazyFrame:
    return daily_table.head(1).lazy()


@asset
def day_last_row(
    context: AssetExecutionContext, single_day: pl.LazyFrame
) -> pl.LazyFrame:
    context.log.info(f"N tickers: {single_day.collect().height}")
    return single_day.tail(1)


@asset(required_resource_keys={"duckdb_config"})
def output_table(
    context: AssetExecutionContext, first_row: pl.LazyFrame, day_last_row: pl.LazyFrame
) -> MaterializeResult:
    db_config = context.resources.duckdb_config
    out_base = pl.concat([first_row, day_last_row]).collect()
    out = out_base.with_columns(_etl_loaded_at=datetime.datetime.now())
    write_table_duckdb("dagster_play_output", out, db_config)

    schema = [TableColumn(name=n, type=str(t)) for n, t in out.schema.items()]

    return MaterializeResult(
        metadata={
            "num_records": out.height,
            "dagster/row_count": MetadataValue.int(out.height),
            "dagster/column_schema": TableSchema(columns=schema),
            "preview": MetadataValue.md(out.head().to_pandas().to_markdown()),
        }
    )


# @asset
# def output_file(context: AssetExecutionContext, first_row: pl.LazyFrame, day_last_row: pl.LazyFrame) -> None:
#     out = pl.concat([first_row, day_last_row])
#     out.collect().write_csv("output.csv")

all_assets_job = define_asset_job(name="all_assets_job")

defs = Definitions(
    assets=[daily_table, single_day, first_row, day_last_row, output_table],
    resources={
        "duckdb_config": duckdb_resource,
    },
    jobs=[all_assets_job],
)
