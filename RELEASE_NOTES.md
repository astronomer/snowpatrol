
Version 2.0.0

### What's changed:
- Added data validation in the data_preparation dag.
- Added Drift detection with Evidently. When data drifts, we retrain the warehouse.
- Renamed the tables to match with what the Data Team uses
- Moved the seasonal_decompose to it's own task in the data_preparation dag. 
  Previously we would run the seasonal_decompose code twice, once in training and once in predict
- Added error handling in critical places
- Added average & standard deviation in the metrics table. 
  End goal is to have a page in Airflow with dashboards. 
  One chart will plot the anomalies with bollinger bands.
  One table will allow users to annotate anomalies. 
  We can then use this to retrain the model in a more supervised approach.
- Added pre-commit hooks to lint and format with Ruff and remove outputs from notebooks
- Changing the name to snowsurge. No-one remembers Snowstorm and it's not fitting with what the solution does.