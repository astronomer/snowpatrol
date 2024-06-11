from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta

from include.datasets import (
    common_calendar_table,
    feature_warehouse_metering_table,
    metrics_warehouse_metering_table,
    model_output_anomalies_table,
    raw_warehouse_metering_history_table,
    reporting_database_storage_cost_table,
    reporting_query_history_table,
    reporting_warehouse_credits_table,
    view_reporting_anomalies,
    view_reporting_dag_cost,
    view_reporting_warehouse_credits_delta,
    view_reporting_dag_cost_warnings,
    view_reporting_dag_cost_delta,
)

# Snowflake Configuration
snowflake_conn_id = "snowflake_conn"

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
        task_id="common_calendar",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/common_calendar_table.sql",
        params={"table_name": common_calendar_table.uri},
    )

    raw_warehouse_metering_history = SQLExecuteQueryOperator(
        task_id="raw_warehouse_metering_history",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/raw_warehouse_metering_history.sql",
        params={"table_name": raw_warehouse_metering_history_table.uri},
    )

    metrics_warehouse_metering_history = SQLExecuteQueryOperator(
        task_id="metrics_warehouse_metering_history",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/metrics_warehouse_metering_history.sql",
        params={"table_name": metrics_warehouse_metering_table.uri},
    )

    feature_metering_seasonal_decompose = SQLExecuteQueryOperator(
        task_id="feature_metering_seasonal_decompose",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/feature_warehouse_metering.sql",
        params={"table_name": feature_warehouse_metering_table.uri},
    )

    model_output_anomalies = SQLExecuteQueryOperator(
        task_id="model_output_anomalies",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/model_output_anomalies.sql",
        params={"table_name": model_output_anomalies_table.uri},
    )

    reporting_warehouse_credits = SQLExecuteQueryOperator(
        task_id="reporting_warehouse_credits",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/reporting_warehouse_credits.sql",
        params={"table_name": reporting_warehouse_credits_table.uri},
    )

    reporting_query_history = SQLExecuteQueryOperator(
        task_id="reporting_query_history",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/reporting_query_history.sql",
        params={"table_name": reporting_query_history_table.uri},
    )

    reporting_database_storage_cost = SQLExecuteQueryOperator(
        task_id="reporting_database_storage_cost",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/reporting_database_storage_cost.sql",
        params={"table_name": reporting_database_storage_cost_table.uri},
    )

    # CREATE VIEWS
    reporting_dag_cost = SQLExecuteQueryOperator(
        task_id="reporting_dag_cost",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/view_reporting_dag_cost.sql",
        params={
            "view_name": view_reporting_dag_cost.uri,
            "reporting_query_history": reporting_query_history_table.uri,
            "reporting_warehouse_credits": reporting_warehouse_credits_table.uri,
        },
    )

    reporting_dag_cost_delta = SQLExecuteQueryOperator(
        task_id="reporting_dag_cost_delta",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/view_reporting_dag_cost_delta.sql",
        params={
            "view_name": view_reporting_dag_cost_delta.uri,
            "reporting_dag_cost": view_reporting_dag_cost.uri,
        },
    )

    reporting_dag_cost_warnings = SQLExecuteQueryOperator(
        task_id="reporting_dag_cost_warnings_view",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/view_reporting_dag_cost_warnings.sql",
        params={
            "view_name": view_reporting_dag_cost_warnings.uri,
            "reporting_dag_cost_delta": view_reporting_dag_cost_delta.uri,
        },
    )

    reporting_warehouse_credits_delta = SQLExecuteQueryOperator(
        task_id="reporting_warehouse_credits_delta",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/view_reporting_warehouse_credits_delta.sql",
        params={
            "view_name": view_reporting_warehouse_credits_delta.uri,
            "reporting_warehouse_credits": reporting_warehouse_credits_table.uri,
        },
    )

    reporting_anomalies = SQLExecuteQueryOperator(
        task_id="reporting_anomalies",
        conn_id=snowflake_conn_id,
        sql="sql/initial_setup/view_reporting_anomalies.sql",
        params={
            "view_name": view_reporting_anomalies.uri,
            "metrics_metering_table": metrics_warehouse_metering_table.uri,
            "model_output_anomalies_table": model_output_anomalies_table.uri,
        },
    )

    tables = EmptyOperator(task_id="create_tables")
    views = EmptyOperator(task_id="create_views")
    end = EmptyOperator(task_id="end")

    # Create Tables first, then Views
    (
        tables
        >> [
            common_calendar,
            feature_metering_seasonal_decompose,
            metrics_warehouse_metering_history,
            model_output_anomalies,
            raw_warehouse_metering_history,
            reporting_query_history,
            reporting_warehouse_credits,
        ]
        >> views
        >> [
            reporting_dag_cost,
            reporting_dag_cost_delta,
            reporting_database_storage_cost,
            reporting_anomalies,
        ]
        >> reporting_dag_cost_warnings
        >> reporting_warehouse_credits_delta
        >> end
    )
