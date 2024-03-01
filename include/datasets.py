# Description: This file contains the dataset definitions for all the Dags in the project.
from airflow import Dataset


source_metering_table = Dataset(
    uri="SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY"
)
raw_metering_table = Dataset(uri="SANDBOX.OLIVIERDANEAU.RAW_WAREHOUSE_METERING_HISTORY")

common_calendar_table = Dataset(uri="SANDBOX.OLIVIERDANEAU.COMMON_CALENDAR")

metrics_metering_table = Dataset(
    uri="SANDBOX.OLIVIERDANEAU.METRICS_WAREHOUSE_METERING_HISTORY"
)
feature_metering_table = Dataset(
    uri="SANDBOX.OLIVIERDANEAU.FEATURE_METERING_SEASONAL_DECOMPOSE"
)

isolation_forest_model = Dataset(uri="isolation_forest_model")

model_output_anomalies_table = Dataset(uri="SANDBOX.OLIVIERDANEAU.MODEL_OUTPUT_ANOMALIES")

labeller_metering_table = Dataset(uri="metering")
labeller_anomaly_table = Dataset(uri="anomaly")
labeller_annotation_table = Dataset(uri="annotation")

reporting_query_duration = Dataset(uri="SANDBOX.OLIVIERDANEAU.REPORTING_QUERY_DURATION")
reporting_storage_cost = Dataset(uri="SANDBOX.OLIVIERDANEAU.REPORTING_STORAGE_COST")