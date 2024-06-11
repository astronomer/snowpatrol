# Description: This file contains the dataset definitions for all the Dags in the project.
from airflow import Dataset

from include.config import db, schema

# Source Tables
source_warehouse_metering_history_table = Dataset(
    uri="SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY"
)

# Raw staging tables
raw_warehouse_metering_history_table = Dataset(
    uri=f"{db}.{schema}.RAW_WAREHOUSE_METERING_HISTORY"
)

# Common reusable tables
common_calendar_table = Dataset(uri=f"{db}.{schema}.COMMON_CALENDAR")

# Metrics Layer Tables
metrics_warehouse_metering_table = Dataset(
    uri=f"{db}.{schema}.METRICS_WAREHOUSE_METERING_HISTORY"
)

# Feature Engineering Tables used to feed the model
feature_warehouse_metering_table = Dataset(
    uri=f"{db}.{schema}.FEATURE_METERING_SEASONAL_DECOMPOSE"
)

# Model
isolation_forest_model = Dataset(uri="isolation_forest_model")

# Model Output Tables
model_output_anomalies_table = Dataset(uri=f"{db}.{schema}.MODEL_OUTPUT_ANOMALIES")

# Reporting Tables
reporting_warehouse_credits_table = Dataset(
    uri=f"{db}.{schema}.REPORTING_WAREHOUSE_CREDITS"
)
reporting_query_history_table = Dataset(uri=f"{db}.{schema}.REPORTING_QUERY_HISTORY")
reporting_database_storage_cost_table = Dataset(
    uri=f"{db}.{schema}.REPORTING_DATABASE_STORAGE_COST"
)

# Reporting Views
view_reporting_dag_cost = Dataset(uri=f"{db}.{schema}.VIEW_REPORTING_DAG_COST")
view_reporting_dag_cost_delta = Dataset(
    uri=f"{db}.{schema}.VIEW_REPORTING_DAG_COST_DELTA"
)
view_reporting_warehouse_credits_delta = Dataset(
    uri=f"{db}.{schema}.VIEW_REPORTING_WAREHOUSE_CREDITS_DELTA"
)
view_reporting_dag_cost_warnings = Dataset(
    uri=f"{db}.{schema}.VIEW_REPORTING_DAG_COST_WARNINGS"
)
view_reporting_anomalies = Dataset(uri=f"{db}.{schema}.VIEW_REPORTING_ANOMALIES")
