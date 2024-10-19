import streamlit as st
from nbconvert import HTMLExporter
import nbformat
from pathlib import Path

def display_notebook(notebook_path):
    """Displays the HTML representation of the Jupyter Notebook."""

    notebook_path = Path(notebook_path)

    try:
        with open(notebook_path, 'r') as f:
            nb = nbformat.read(f, as_version=4)

        html_exporter = HTMLExporter()
        body, _ = html_exporter.from_notebook_node(nb)
        st.html(body)

    except FileNotFoundError:
        st.error(f"Could not find the notebook file: {notebook_path}")
