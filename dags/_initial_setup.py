from datetime import timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta

from include.datasets import (
    common_calendar_table,
    feature_metering_table,
    metrics_metering_table,
    model_output_anomalies_table,
    raw_metering_table,
    reporting_query_duration,
    reporting_storage_cost,
)

# Snowflake Configuration
snowflake_conn_id = "snowflake_admin"

doc_md = """
# Initial Setup
Creates all the necessary Tables used by the SnowPatrol project.
"""

with DAG(
    dag_id="_initial_setup",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=None,
    start_date=timezone.utcnow() - relativedelta(years=+1),
    catchup=False,
    max_active_runs=1,
    doc_md=doc_md,
    template_searchpath="/usr/local/airflow/include",
):
    common_calendar = SQLExecuteQueryOperator(
        task_id="create_common_calendar_table",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_common_calendar_table.sql",
        params={"table_name": common_calendar_table.uri},
    )
    raw_warehouse_metering_history = SQLExecuteQueryOperator(
        task_id="create_raw_warehouse_metering_history",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_raw_warehouse_metering_history.sql",
        params={"table_name": raw_metering_table.uri},
    )

    metrics_warehouse_metering_history = SQLExecuteQueryOperator(
        task_id="create_metrics_warehouse_metering_history",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_metrics_warehouse_metering_history.sql",
        params={"table_name": metrics_metering_table.uri},
    )
    feature_metering_seasonal_decompose = SQLExecuteQueryOperator(
        task_id="create_feature_metering_seasonal_decompose",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_feature_metering_seasonal_decompose.sql",
        params={"table_name": feature_metering_table.uri},
    )

    model_output_anomalies = SQLExecuteQueryOperator(
        task_id="create_model_output_anomalies",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_model_output_anomalies.sql",
        params={"table_name": model_output_anomalies_table.uri},
    )
    reporting_query_duration = SQLExecuteQueryOperator(
        task_id="create_reporting_query_duration",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_reporting_query_duration.sql",
        params={"table_name": reporting_query_duration.uri},
    )
    reporting_storage_cost = SQLExecuteQueryOperator(
        task_id="create_reporting_storage_cost",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/create_reporting_storage_cost.sql",
        params={"table_name": reporting_storage_cost.uri},
    )

    common_calendar
    raw_warehouse_metering_history
    metrics_warehouse_metering_history
    feature_metering_seasonal_decompose
    model_output_anomalies
    reporting_query_duration
    reporting_storage_cost
