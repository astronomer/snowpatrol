from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago

_SNOWFLAKE_CONN_ID = "snowflake_admin"

currency_table = Dataset(uri="USAGE_IN_CURRENCY_DAILY")


@dag(
    default_args={},
    schedule=None,
    start_date=days_ago(2),
)
def data_engineering():
    """
    This DAG performs Extract, Transform and Loading of data from sources into tables for upstream ML DAGs.
    """

    @task.snowpark_python(snowflake_conn_id=_SNOWFLAKE_CONN_ID, outlets=currency_table)
    def load_account_data(currency_table_name:str):
        """
        This task is a placeholder.  Snowflake data engineering tasks populate the table
        SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY.  Here we simply extract that to the 
        project database/schema for downstream feature engineering.
        """        

        currency_usage = snowpark_session.table(f"SNOWFLAKE.ORGANIZATION_USAGE.{currency_table_name}")
        
        currency_usage.write.save_as_table(currency_table_name, mode="overwrite")


    load_account_data(currency_table_name=currency_table.uri)

data_engineering()