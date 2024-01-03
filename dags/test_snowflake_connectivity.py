from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago


@dag(
    default_args={},
    schedule=None,
    start_date=days_ago(2),
)
def connectivity_test():
    """
    Test connectivity to snowflake_admin and snowflake_ro.
    """
    SQLExecuteQueryOperator(
        task_id="test_snowflake_admin_conn",
        conn_id="snowflake_admin",
        sql="SELECT TOP 0 * FROM ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY;",
    )

    SQLExecuteQueryOperator(
        task_id="test_snowflake_ro_conn",
        conn_id="snowflake_ro",
        sql="SELECT TOP 0 * FROM DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY;",
    )


connectivity_test()
