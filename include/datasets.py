# Description: This file contains the dataset definitions for all the Dags in the project.
import os
from airflow import Dataset

db = os.getenv("SNOWFLAKE_DATASET_DB")
schema = os.getenv("SNOWFLAKE_DATASET_SCHEMA")

source_metering_table = Dataset(
    uri="SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY"
)
raw_metering_table = Dataset(uri=f"{db}.{schema}.RAW_WAREHOUSE_METERING_HISTORY")

common_calendar_table = Dataset(uri=f"{db}.{schema}.COMMON_CALENDAR")

metrics_metering_table = Dataset(
    uri=f"{db}.{schema}.METRICS_WAREHOUSE_METERING_HISTORY"
)
feature_metering_table = Dataset(
    uri=f"{db}.{schema}.FEATURE_METERING_SEASONAL_DECOMPOSE"
)

isolation_forest_model = Dataset(uri="isolation_forest_model")

model_output_anomalies_table = Dataset(uri=f"{db}.{schema}.MODEL_OUTPUT_ANOMALIES")

labeller_metering_table = Dataset(uri="metering")
labeller_anomaly_table = Dataset(uri="anomaly")
labeller_annotation_table = Dataset(uri="annotation")

reporting_query_duration = Dataset(uri=f"{db}.{schema}.REPORTING_QUERY_DURATION")
reporting_storage_cost = Dataset(uri=f"{db}.{schema}.REPORTING_STORAGE_COST")
