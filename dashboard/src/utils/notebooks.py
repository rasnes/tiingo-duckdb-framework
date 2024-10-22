import io
import streamlit as st
from nbconvert import HTMLExporter
import nbformat
from pathlib import Path
from utils import gdrive


def download_and_display_notebook(py_path: Path) -> None:
    downloaded_file = download_notebook(py_path)
    if downloaded_file:
        display_downloaded_notebook(downloaded_file)

def download_notebook(py_path: Path) -> io.BytesIO | None:
    file_to_download = f"{py_path.stem}.ipynb"

    service = gdrive.create_drive_service()
    folder_id = '1LJb30Ua3LLPsllqi2kWm5fyxGSNlENqH'
    files = gdrive.list_files(service, folder_id)
    file_id = [file['id'] for file in files if file['name'] == file_to_download]
    try:
        file_id = file_id[0]
    except IndexError:
        st.error(f"Could not find the notebook file in GDrive: {file_to_download}")
        return None

    return gdrive.download_file(service, file_id)


def display_downloaded_notebook(file_content: io.BytesIO) -> None:
    """Displays the HTML representation of a Jupyter Notebook from a BytesIO object."""
    try:
        # Reset buffer position to start
        file_content.seek(0)

        # Read notebook directly from BytesIO
        nb = nbformat.read(file_content, as_version=4)

        html_exporter = HTMLExporter()
        body, _ = html_exporter.from_notebook_node(nb)
        st.html(body)

    except Exception as e:
        st.error(f"Error displaying notebook: {str(e)}")


def display_local_notebook(py_path: Path) -> None:
    """Displays the HTML representation of the Jupyter Notebook."""

    notebook_path = py_path.parent / f"{py_path.stem}.ipynb"

    try:
        with open(notebook_path, "r") as f:
            nb = nbformat.read(f, as_version=4)

        html_exporter = HTMLExporter()
        body, _ = html_exporter.from_notebook_node(nb)
        st.html(body)

    except FileNotFoundError:
        st.error(f"Could not find the notebook file: {notebook_path}")
