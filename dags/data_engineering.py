from airflow.decorators import dag, task
from airflow import Dataset
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from textwrap import dedent
from airflow.utils.dates import days_ago

_SNOWFLAKE_CONN_ID = "snowflake_admin"

usage_table = Dataset(uri="USAGE_IN_CURRENCY_DAILY")


@dag(
    default_args={},
    schedule=None,
    start_date=days_ago(2),
)
def data_engineering():
    """
    This DAG performs Extract, Transform and Loading of data from sources into tables for upstream ML DAGs.
    """

    SnowflakeOperator(
        doc_md=dedent("""
            This task is a placeholder.  Snowflake data engineering tasks populate the table
            SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY.  Here we simply extract that to the 
            project database/schema for downstream feature engineering."""),
        task_id="load_account_data",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID, 
        outlets=usage_table,
        sql=f"""
            CREATE OR REPLACE TABLE {usage_table.uri}(
                "ACCOUNT_NAME" STRING(16777216), 
                "USAGE_DATE" DATE, 
                "USAGE_TYPE" STRING(16777216), 
                "CURRENCY" STRING(16777216), 
                "USAGE" NUMBER(38, 6), 
                "USAGE_IN_CURRENCY" NUMBER(38, 6), 
                "USAGE_TYPE_CLEAN" STRING(16777216)
            )  AS  SELECT  
                ACCOUNT_NAME, 
                USAGE_DATE, 
                USAGE_TYPE, 
                CURRENCY, 
                USAGE, 
                USAGE_IN_CURRENCY, 
                REGEXP_REPLACE(USAGE_TYPE, '( ){{1,}}', '_') AS USAGE_TYPE_CLEAN 
            FROM SNOWFLAKE.ORGANIZATION_USAGE.{usage_table.uri}"""    
    )

data_engineering()