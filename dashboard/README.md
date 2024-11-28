# dashboard

## TODOs

- Upgrade Python to 3.12, ditch catboost in dashboard
  - Rather, get all model results from duckdb directly.
  - Because: memory would be very hard to keep below 1GB and dealing with model versions via files would be messy.
- New tab: model overview
  - Metadata for model
  - Shapley value plot for features
  - Github Artifacts has max retention of 90 days = 12 weeks of models
    - Include historic developement of test set metrics: RMSE, ?
    - To start with: only include an overview over model performance metrics, i.e. manual monitoring.
      - Automatic monitoring could be played with later (with what stack?)
  - UPDATE: I think I should generate all model-level metadata stats as part of the pipeline, and not by using the artifact + data inside streamlit, as it will require both bandwith, memory and compute.
- Hmm, is there any way to include the artifacts in Streamlit build time?
