# transformations

After obtaining latest data, all data transformations and ML workloads should be orchestrated with Dagster, is defined in this directory.

## TODO

- Configure training pipeline for the 3 model trainings
  - They may run in parallell, not sure it hurts. Could be memory issues in Github Actions though.
  - Consider making the seed random, from e.g. a list of 10 different ints.
- Create or change methods for:
  - When done with Polars schema, create duckdb table and insert or replace values (create if not exist table) and add an index for fast lookups.
- Create 3 "export quality" notebooks, with each models feature importances and shap_beeswarm plots.
  - Keep documentation very brief, but link to the class used in the notebook.
  - Create a dedicated export of model stats function, on test datasets
    - Includes: num_rows, num_tickers, column_names and key stats per column, test set RMSE, test set MAPE, test set R2, train-val-test set split sizes
- Later: Create table for key MLOps stats
  - Test set RMSE, training date, num_rows, num_cols, pred_col, catboost iterations, catboost model params, train-val-test set split sizes
  - This should get a dedicated page in the dashboard.
- Later: Try training once without NVDA for 36m, and see how well the historic predictions behave.
- Later: deploy pipeline to github actions (focus FIRST on getting something useful in Streamlit from artifacts + data)
