from pathlib import Path
from utils import notebooks

# --- Get notebook path relative to eda.py ---
notebook_path = Path(__file__).parent / "eda.ipynb"
notebooks.display_notebook(notebook_path)
