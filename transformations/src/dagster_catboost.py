from os import environ
import datetime
from dagster import (
    Definitions,
    asset,
    resource,
    AssetExecutionContext,
    Output,
    MetadataValue,
    TableColumn,
    TableSchema,
    define_asset_job,
)
import polars as pl
import duckdb
from pathlib import Path
import pyarrow as pa
import pandas as pd

LOCAL = True if environ["APP_ENV"] == "dev" else False

@resource(config_schema={"local": bool})
def duckdb_resource(init_context):
    local = init_context.resource_config["local"]
    if local:
        # Create a data directory if it doesn't exist
        data_dir = Path("src")
        data_dir.mkdir(exist_ok=True)
        return {
            "database": str(data_dir / "prod_copy_2024-11-08.db"),  # Local file-based database
            "schema": "main",
            "access_mode": "READ_WRITE",
        }
    else:
        return {
            "database": f"""md:{environ["APP_ENV"]}?motherduck_token={environ["MOTHERDUCK_TOKEN"]}""",
            "schema": "main",
        }


def duckdb_query_polars(sql: str, db_config: dict[str, str]) -> pl.DataFrame:
    """Query a DuckDB database and return the result as a Polars DataFrame """
    con = duckdb.connect(database=db_config["database"], read_only=True)
    try:
        df = con.query(sql).pl()
        return df
    finally:
        con.close()


def duckdb_query(sql: str, db_config: dict[str, str]) -> None:
    """Query a DuckDB database to create macros, views and tables"""
    con = duckdb.connect(database=db_config["database"], read_only=False)
    con.query(sql)
    con.close()


def write_table_duckdb(
    table_name: str, df: pl.DataFrame, db_config: dict[str, str]
) -> None:
    df.write_database(
        table_name=table_name,
        connection=f"duckdb:///{db_config['database']}",
        if_table_exists="replace",
    )


def query_duckdb_file(file: Path, db_config: dict[str, str]) -> None:
    sql = file.read_text()
    duckdb_query(sql, db_config)


@asset(required_resource_keys={"duckdb_config"})
def load_macros(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/macros.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[load_macros],
)
def table_wide_statements(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/1_wide_statements.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[table_wide_statements],
)
def view_wide_with_daily_fundamentals(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/2_wide_with_daily_fundamentals.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[view_wide_with_daily_fundamentals]
)
def view_wide_with_combined_metrics(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/3_wide_with_combined_metrics.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[view_wide_with_combined_metrics]
)
def table_excess_returns(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/4_excess_returns.sql"), db_config)




@asset(
    required_resource_keys={"duckdb_config"},
    deps=[table_excess_returns]
)
def excess_returns(context: AssetExecutionContext) -> Output:
    db_config = context.resources.duckdb_config

    # Be explicit about the schema
    df = duckdb_query_polars("SELECT * FROM fundamentals.excess_returns", db_config)

    schema = [TableColumn(name=n, type=str(t)) for n, t in df.schema.items()]
    size_mb = df.estimated_size() / (1024 * 1024)

    return Output(
        value=df,
        metadata={
            "num_records": df.height,
            "dagster/row_count": MetadataValue.int(df.height),
            "dagster/column_schema": TableSchema(columns=schema),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "size_mb": MetadataValue.float(round(size_mb, 2)),
        }
    )



########## Dagster Job ##########

catboost = define_asset_job(name="catboost")

defs = Definitions(
    assets=[
        load_macros,
        table_wide_statements,
        view_wide_with_daily_fundamentals,
        view_wide_with_combined_metrics,
        table_excess_returns,
        excess_returns,
    ],
    resources={
        "duckdb_config": duckdb_resource.configured({"local": LOCAL})
    },
    jobs=[catboost],
)
