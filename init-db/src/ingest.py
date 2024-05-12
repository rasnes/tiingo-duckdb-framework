import duckdb
import pathlib


class Ingest:
    def __init__(self, db_path: str, sql_dir: str = "sql"):
        self.con = duckdb.connect(db_path)
        self.cur = self.con.cursor()
        self.sql_dir = pathlib.Path(sql_dir)

    def create_table(self, file_name: str = "create_table.sql"):
        sql_file = self.sql_dir / file_name
        sql = sql_file.read_text()
        self.cur.execute(sql)

    def ingest_data(self, dst_table: str, data: str):
        self.cur.execute(f"copy {dst_table} from '{data}'")

    def close(self):
        self.con.close()

    def commit(self):
        self.con.commit()
