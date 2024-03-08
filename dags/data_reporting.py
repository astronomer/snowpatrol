from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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
):
    load_reporting_query_duration = SQLExecuteQueryOperator(
        task_id="load_reporting_query_duration",
        conn_id=snowflake_conn_id,
        outlets=reporting_query_duration,
        sql=f"""
            CREATE OR REPLACE TABLE {reporting_query_duration.uri} AS
            WITH warehouse_sizes AS (
                SELECT 'X-Small'  AS warehouse_size, 1   AS credits_per_hour UNION ALL
                SELECT 'Small'    AS warehouse_size, 2   AS credits_per_hour UNION ALL
                SELECT 'Medium'   AS warehouse_size, 4   AS credits_per_hour UNION ALL
                SELECT 'Large'    AS warehouse_size, 8   AS credits_per_hour UNION ALL
                SELECT 'X-Large'  AS warehouse_size, 16  AS credits_per_hour UNION ALL
                SELECT '2X-Large' AS warehouse_size, 32  AS credits_per_hour UNION ALL
                SELECT '3X-Large' AS warehouse_size, 64  AS credits_per_hour UNION ALL
                SELECT '4X-Large' AS warehouse_size, 128 AS credits_per_hour
            )
            SELECT
                qh.query_id,
                qh.query_text,
                qh.database_name,
                qh.schema_name,
                qh.warehouse_name,
                qh.warehouse_size,
                qh.warehouse_type,
                qh.user_name,
                qh.role_name,
                DATE(qh.start_time) as start_date,
                qh.error_code,
                qh.execution_status,
                qh.execution_time/(1000) AS execution_time_sec,
                qh.total_elapsed_time/(1000) AS total_elapsed_time_sec,
                qh.rows_deleted,
                qh.rows_inserted,
                qh.rows_produced,
                qh.rows_unloaded,
                qh.rows_updated,
                qh.execution_time/(1000*60*60)*wh.credits_per_hour AS credits_cost,
                credits_cost * 2.00 AS dollars_cost, -- We use Snowflake Standard. See pricing: https://www.snowflake.com/en/data-cloud/pricing-options/
                SYSDATE() AS last_updated_at
            FROM snowflake.account_usage.query_history AS qh
            INNER JOIN warehouse_sizes AS wh
                ON qh.warehouse_size=wh.warehouse_size
        """,
    )

    load_reporting_storage_cost = SQLExecuteQueryOperator(
        task_id="load_reporting_storage_cost",
        conn_id=snowflake_conn_id,
        outlets=reporting_storage_cost,
        sql=f"""
       CREATE OR REPLACE TABLE {reporting_storage_cost.uri} AS
        SELECT database_name,
            database_id,
            AVERAGE_DATABASE_BYTES AS database_bytes,
            (database_bytes / POWER(1024, 4)) * 23.00 AS database_storage_cost,
            SYSDATE() AS last_updated_at
        FROM snowflake.account_usage.database_storage_usage_history
        """,
    )

    load_reporting_query_duration
    load_reporting_storage_cost
