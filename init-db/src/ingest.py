import duckdb
import pathlib


class Ingest:
    """Class to ingest data into a DuckDB/Motherduck database."""

    def __init__(self, db_path: str, sql_dir: str = "sql"):
        """Initializes the Ingest object.

        Parameters
        ----------
        db_path : str
            The path to the database.
        sql_dir : str, optional
            The directory where the SQL files are stored, by default "sql".
        """
        self.con = duckdb.connect(db_path)
        self.cur = self.con.cursor()
        self.sql_dir = pathlib.Path(sql_dir)

    def create_table(self, file_name: str = "create_table.sql") -> None:
        """Creates a table in the database.

        Parameters
        ----------
        file_name : str, optional
            The name of the SQL file containing the table creation query,
            by default "create_table.sql".
        """
        sql_file = self.sql_dir / file_name
        sql = sql_file.read_text()
        self.cur.execute(sql)

    def ingest_data(self, dst_table: str, data: str) -> None:
        """Ingests data into the database.

        Parameters
        ----------
        dst_table : str
            The name of the destination table.
        data : str
            The path to the data file or files to ingest. Accepts wildcards, e.g. "/data/*.csv."
        """
        self.cur.execute(f"copy {dst_table} from '{data}'")

    def close(self):
        """Closes the connection to the database."""
        self.con.close()
