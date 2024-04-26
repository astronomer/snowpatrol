from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

snowflake_conn_id = "snowflake_conn"
postgres_conn_id = "postgres_conn"

snowflake_doc_md = """
# Connectivity Test
Tests connectivity to the Databases.
"""


@dag(default_args={}, schedule=None, start_date=days_ago(1), doc_md=snowflake_doc_md)
def test_connectivity():
    snowflake = SQLExecuteQueryOperator(
        doc_md="This tests the Snowflake connection without returning any data.",
        task_id="test_snowflake_conn",
        conn_id=snowflake_conn_id,
        sql="SELECT TOP 0 * FROM information_schema.tables;",
    )
    postgres = SQLExecuteQueryOperator(
        doc_md="This tests the Postgres connection without returning any data.",
        task_id="test_postgres_conn",
        conn_id=postgres_conn_id,
        sql="SELECT TOP 0 * FROM information_schema.tables;",
    )

    snowflake
    postgres
