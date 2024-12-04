# transformations

After obtaining latest data, all data transformations and ML workloads should be orchestrated with Dagster, is defined in this directory.

## TODO

- Fix too many NULL values in volatility and other technical metrics
  - Volatility: consider going back further in time to "detect" large drops in value.
  - Also: for volatility and other metrics that might not be complete for certain companies, e.g. just 9 months of data is available which results in NULL when calculating 12m variance.
    - Maybe it is better with, in this case, use the 9 months and calculate instead of returning NULL.
- Configure training pipeline for the 3 model trainings
  - They may run in parallell, not sure it hurts. Could be memory issues in Github Actions though.
  - Consider making the seed random, from e.g. a list of 10 different ints.
- Create 3 "export quality" notebooks, with each models feature importances and shap_beeswarm plots.
  - Keep documentation very brief, but link to the class used in the notebook.
  - Create a dedicated export of model stats function, on test datasets
    - Includes: num_rows, num_tickers, column_names and key stats per column, test set RMSE, test set MAPE, test set R2, train-val-test set split sizes
- Later: predict probability larger than 1, by measuring density over 1.
- Later: Create table for key MLOps stats
  - Test set RMSE, training date, num_rows, num_cols, pred_col, catboost iterations, catboost model params, train-val-test set split sizes
  - This should get a dedicated page in the dashboard.
- Later: Try training once without NVDA for 36m, and see how well the historic predictions behave.
- Later: deploy pipeline to github actions (focus FIRST on getting something useful in Streamlit from artifacts + data)
