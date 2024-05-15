import pathlib
from unittest.mock import MagicMock
from src.ingest import Ingest


def test_ingest_initialization(mocker):
    # Mock the duckdb.connect method
    mock_connect = mocker.patch("src.ingest.duckdb.connect", return_value=MagicMock())

    # Create an instance of Ingest
    db_path = "test.db"
    sql_dir = "sql"
    ingest = Ingest(db_path, sql_dir)

    # Assertions
    mock_connect.assert_called_once_with(db_path)
    assert ingest.sql_dir == pathlib.Path(sql_dir)


def test_create_table(mocker):
    # Mock the duckdb connection and cursor
    mock_con = MagicMock()
    mock_cur = MagicMock()
    mock_con.cursor.return_value = mock_cur
    mocker.patch("src.ingest.duckdb.connect", return_value=mock_con)

    # Mock the read_text method of pathlib.Path
    mock_read_text = mocker.patch(
        "pathlib.Path.read_text", return_value="CREATE TABLE test (id INT);"
    )

    # Create an instance of Ingest
    ingest = Ingest("test.db")

    # Call the create_table method
    ingest.create_table("create_table.sql")

    # Assertions
    mock_read_text.assert_called_once_with()
    mock_cur.execute.assert_called_once_with("CREATE TABLE test (id INT);")


def test_ingest_data(mocker):
    # Mock the duckdb connection and cursor
    mock_con = MagicMock()
    mock_cur = MagicMock()
    mock_con.cursor.return_value = mock_cur
    mocker.patch("src.ingest.duckdb.connect", return_value=mock_con)

    # Create an instance of Ingest
    ingest = Ingest("test.db")

    # Call the ingest_data method
    dst_table = "test_table"
    data = "/data/*.csv"
    ingest.ingest_data(dst_table, data)

    # Assertions
    mock_cur.execute.assert_called_once_with(f"copy {dst_table} from '{data}'")


def test_close(mocker):
    # Mock the duckdb connection
    mock_con = MagicMock()
    mocker.patch("src.ingest.duckdb.connect", return_value=mock_con)

    # Create an instance of Ingest
    ingest = Ingest("test.db")

    # Call the close method
    ingest.close()

    # Assertions
    mock_con.close.assert_called_once_with()
