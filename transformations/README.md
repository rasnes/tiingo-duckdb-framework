# transformations

After obtaining latest data, all data transformations and ML workloads should be orchestrated with Dagster, is defined in this directory.

## TODO

- Tidy up utils
- Create dagster DAG that trains 12, 24 and 36 models and uploads them to artifact registry.
  - Hmm, is there any way to include the artifacts in Streamlit build time?
- Later: deploy pipeline to github actions (focus FIRST on getting something useful in Streamlit from artifacts + data)
