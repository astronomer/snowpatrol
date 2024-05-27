from datetime import timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta

from include.config import account_number
from include.datasets import (
    raw_warehouse_metering_history_table,
    source_warehouse_metering_history_table,
)

# Snowflake Configuration
snowflake_conn_id = "snowflake_conn"
snowflake_hook = SnowflakeHook(snowflake_conn_id)

# Slack Configuration
slack_conn_id = "slack_alert"
slack_channel = "#snowstorm-alerts"

doc_md = f"""
        # Data Ingestion
        This DAG performs the data ingestion for Snowflake's Metering view. The last 365 days are loaded.

        #### Tables
        - [{source_warehouse_metering_history_table.uri}](https://docs.snowflake.com/en/sql-reference/organization-usage/warehouse_metering_history)
        - {raw_warehouse_metering_history_table.uri} - A source table to accumulate the metering data
        """

with DAG(
    dag_id="data_ingestion",
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
    load_warehouse_metering_history = SQLExecuteQueryOperator(
        doc_md="""
            Task to persist the data from the view
            SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY.
            This view keeps the last 365 days of data so we need to persist it ourselves.
            We reload 3 days each time to account for Snowflake past corrections.
            """,
        task_id="load_warehouse_metering_history_table",
        conn_id=snowflake_conn_id,
        outlets=raw_warehouse_metering_history_table,
        sql="sql/data_ingestion/warehouse_metering_history.sql",
        params={
            "table_name": raw_warehouse_metering_history_table.uri,
            "source_metering_table": source_warehouse_metering_history_table.uri,
            "account_number": account_number,
        },
    )

    load_warehouse_metering_history
