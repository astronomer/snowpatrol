from datetime import timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta

from include.config import account_number
from include.datasets import (
    reporting_database_storage_cost_table,
    reporting_query_history_table,
    reporting_warehouse_credits_table,
)

# Snowflake Configuration
snowflake_conn_id = "snowflake_conn"
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
    start_date=timezone.utcnow() - relativedelta(years=+1),
    catchup=True,
    max_active_runs=10,
    doc_md=doc_md,
    template_searchpath="/usr/local/airflow/include",
):
    # Query history with parsed JSON query tags for the Airflow Metadata.
    load_reporting_query_history = SQLExecuteQueryOperator(
        task_id="load_reporting_query_history_table",
        conn_id=snowflake_conn_id,
        outlets=reporting_query_history_table,
        sql="sql/data_reporting/reporting_query_history.sql",
        params={"reporting_query_history": reporting_query_history_table.uri},
    )

    # Calculating the warehouse credits by using the total time and total number of credits.
    load_reporting_warehouse_credits = SQLExecuteQueryOperator(
        task_id="load_reporting_warehouse_credits_table",
        conn_id=snowflake_conn_id,
        outlets=reporting_warehouse_credits_table,
        sql="sql/data_reporting/reporting_warehouse_credits.sql",
        params={
            "reporting_warehouse_credits": reporting_warehouse_credits_table.uri,
            "account_number": account_number,
        },
    )

    # Compute the storage cost for each Database
    load_reporting_storage_cost = SQLExecuteQueryOperator(
        task_id="load_reporting_storage_cost_table",
        conn_id=snowflake_conn_id,
        outlets=reporting_database_storage_cost_table,
        sql="sql/data_reporting/reporting_database_storage_cost.sql",
        params={
            "reporting_database_storage_cost": reporting_database_storage_cost_table.uri
        },
    )

    load_reporting_warehouse_credits
    load_reporting_query_history
    load_reporting_storage_cost