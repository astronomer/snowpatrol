from datetime import datetime, timedelta
from textwrap import dedent

from airflow import Dataset
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_SNOWFLAKE_CONN_ID = "snowflake_admin"

source_table = Dataset(uri="SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY")
usage_table = Dataset(uri="DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY")


@dag(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@daily",
    start_date=datetime(2022, 12, 1),
    catchup=True,
)
def data_engineering():
    """
    This DAG performs Extract, Transform and Loading of daily data from sources into tables for upstream ML DAGs.
    """

    SQLExecuteQueryOperator(
        doc_md=dedent(
            """
            Task to populate the table DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY 
            from the view SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY.
            We perform a MERGE operation to ensure that each run is idempotent.
            We simply extract source data for downstream feature engineering."""
        ),
        task_id="usage_in_currency_daily",
        conn_id=_SNOWFLAKE_CONN_ID,
        outlets=usage_table,
        sql=f"""
        MERGE INTO {usage_table.uri} AS target
        USING (
            SELECT 
                ACCOUNT_NAME,
                USAGE_DATE,
                USAGE_TYPE,
                CURRENCY,                
                USAGE,
                USAGE_IN_CURRENCY,
                REGEXP_REPLACE(USAGE_TYPE, '( ){{1,}}', '_') AS USAGE_TYPE_CLEAN,
                TO_TIMESTAMP_NTZ('{{{{ ts }}}}') AS UPDATED_AT
            FROM {source_table.uri}
            WHERE USAGE_DATE = '{{{{ ds }}}}'
        ) AS source
        ON  source.ACCOUNT_NAME = target.ACCOUNT_NAME
        AND source.USAGE_DATE   = target.USAGE_DATE
        AND source.USAGE_TYPE   = target.USAGE_TYPE
        AND source.CURRENCY     = target.CURRENCY
        AND source.USAGE_TYPE_CLEAN = target.USAGE_TYPE_CLEAN
        WHEN MATCHED THEN
            UPDATE SET
                target.ACCOUNT_NAME = source.ACCOUNT_NAME,
                target.USAGE_DATE = source.USAGE_DATE,
                target.USAGE_TYPE = source.USAGE_TYPE,
                target.CURRENCY = source.CURRENCY,
                target.USAGE = source.USAGE,
                target.USAGE_IN_CURRENCY = source.USAGE_IN_CURRENCY,
                target.USAGE_TYPE_CLEAN = source.USAGE_TYPE_CLEAN,
                target.UPDATED_AT = source.UPDATED_AT
        WHEN NOT MATCHED THEN
            INSERT (
                ACCOUNT_NAME,
                USAGE_DATE,
                USAGE_TYPE,
                CURRENCY,
                USAGE,
                USAGE_IN_CURRENCY,
                USAGE_TYPE_CLEAN,
                UPDATED_AT
            ) VALUES (
                source.ACCOUNT_NAME,
                source.USAGE_DATE,
                source.USAGE_TYPE,
                source.CURRENCY,
                source.USAGE,
                source.USAGE_IN_CURRENCY,
                source.USAGE_TYPE_CLEAN,
                source.UPDATED_AT
            );
        """,
    )


data_engineering()
