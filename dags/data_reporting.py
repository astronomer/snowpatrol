from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.config import snowflake_credit_cost
from include.datasets import reporting_query_duration, reporting_storage_cost

# Snowflake Configuration
snowflake_conn_id = "snowflake_admin"
snowflake_hook = SnowflakeHook(snowflake_conn_id)

# Slack Configuration
slack_conn_id = "slack_alert"
slack_channel = "#snowstorm-alerts"

doc_md = """
# Data Reporting
Load Tables used by Sigma for reporting.
"""

with DAG(
    dag_id="data_reporting",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": send_slack_notification(
            slack_conn_id="slack_alert",
            text="The task {{ ti.task_id }} failed. Check the logs.",
            channel=slack_channel,
        ),
    },
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=doc_md,
    template_searchpath="/usr/local/airflow/include",
):
    load_reporting_query_duration = SQLExecuteQueryOperator(
        task_id="load_reporting_query_duration",
        conn_id=snowflake_conn_id,
        outlets=reporting_query_duration,
        sql="sql/data_reporting/load_reporting_query_duration.sql",
        params={
            "reporting_query_duration": reporting_query_duration.uri,
            "snowflake_credit_cost": snowflake_credit_cost,
        },
    )

    load_reporting_storage_cost = SQLExecuteQueryOperator(
        task_id="load_reporting_storage_cost",
        conn_id=snowflake_conn_id,
        outlets=reporting_storage_cost,
        sql="sql/data_reporting/load_reporting_storage_cost.sql",
        params={
            "reporting_storage_cost": reporting_storage_cost.uri,
        },
    )

    load_reporting_query_duration
    load_reporting_storage_cost
