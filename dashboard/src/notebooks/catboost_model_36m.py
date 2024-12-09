from pathlib import Path
from utils import notebooks
import streamlit as st

@st.cache_resource
def get_notebook_content(file_path: Path):
  """Downloads and caches the notebook content."""
  return notebooks.download_and_display_notebook(file_path)

get_notebook_content(Path(__file__))
