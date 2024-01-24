# Description: This file contains the dataset definitions for all the Dags in the project.
from airflow import Dataset

calendar_table = Dataset(uri="DEMO.SNOWSTORM.CALENDAR")

source_metering_table = Dataset(
    uri="SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY"
)
raw_metering_table = Dataset(uri="DEMO.SNOWSTORM.RAW_WAREHOUSE_METERING_HISTORY")
metrics_metering_table = Dataset(
    uri="DEMO.SNOWSTORM.METRICS_WAREHOUSE_METERING_HISTORY"
)
feature_metering_table = Dataset(
    uri="DEMO.SNOWSTORM.FEATURE_METERING_SEASONAL_DECOMPOSE"
)

isolation_forest_model = Dataset(uri="isolation_forest_model")

output_anomalies_table = Dataset(uri="DEMO.SNOWSTORM.MODEL_OUTPUT_ANOMALIES")
