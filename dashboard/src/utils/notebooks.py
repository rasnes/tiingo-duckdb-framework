import streamlit as st
from nbconvert import HTMLExporter
import nbformat
from pathlib import Path

def display_notebook(py_path: Path) -> None:
    """Displays the HTML representation of the Jupyter Notebook."""

    notebook_path = py_path.parent / f"{py_path.stem}.ipynb"

    try:
        with open(notebook_path, 'r') as f:
            nb = nbformat.read(f, as_version=4)

        html_exporter = HTMLExporter()
        body, _ = html_exporter.from_notebook_node(nb)
        st.html(body)

    except FileNotFoundError:
        st.error(f"Could not find the notebook file: {notebook_path}")
