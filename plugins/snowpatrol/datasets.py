import json

import snowflake.connector

from plugins.snowpatrol.config import snowflake_conn


def get_conn():
    airflow_con = json.loads(snowflake_conn)
    snowflake_params = {
        "user": airflow_con["login"],
        "password": airflow_con["password"],
        "account": airflow_con["extra"]["account"],
        "region": airflow_con["extra"]["region"],
        "warehouse": airflow_con["extra"]["warehouse"],
        "database": airflow_con["extra"]["database"],
        "schema": airflow_con["schema"],
    }
    conn = snowflake.connector.connect(**snowflake_params)
    return conn


def fetch_data(query: str):
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
    return df


temp_query = """SELECT *
FROM SANDBOX.OLIVIERDANEAU.MODEL_OUTPUT_ANOMALIES"""

temp_df = fetch_data(temp_query)
#
# dag_task_cost_query = """SELECT dag_id,
# task_id,
# execution_count,
# credit_cost,
# avg_cost_per_run
# FROM AIRFLOW_SNOWFLAKE_COST"""
#
# dag_task_cost_df = fetch_data(dag_task_cost_query)
#
# dag_time_series_query = """SELECT dag_id,
# execution_date,
# credit_cost,
# FROM AIRFLOW_SNOWFLAKE_COST
# """
#
# dag_time_series_df = fetch_data(dag_time_series_query)
#
# dag_execution_cost_query = """SELECT dag_id,
# task_id,
# airflow_operator,
# airflow_run_id,
# airflow_started,
# airflow_logical_date,
# credit_cost
# FROM AIRFLOW_SNOWFLAKE_COST"""
#
# dag_execution_cost_df = fetch_data(dag_execution_cost_query)
