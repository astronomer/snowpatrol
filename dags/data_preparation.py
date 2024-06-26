import logging
from datetime import timedelta

import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta
from statsmodels.tsa.seasonal import seasonal_decompose

from include.datasets import (
    common_calendar_table,
    feature_warehouse_metering_table,
    metrics_warehouse_metering_table,
    raw_warehouse_metering_history_table,
)
from include.exceptions import DataValidationFailed

# Snowflake Configuration
snowflake_conn_id = "snowflake_conn"
snowflake_hook = SnowflakeHook(snowflake_conn_id)

# Slack Configuration
slack_conn_id = "slack_alert"
slack_channel = "#snowstorm-alerts"

doc_md = f"""
        # Data Preparation
        This DAG performs the data preparation for Snowflake's Metering data.
        Important: Ensure the Data Ingestion Dag has run for all dates before attempting to run this Dag.

        #### Tables
        - {common_calendar_table.uri} - A simple calendar table populated for 5 years starting on 2023-01-01
        - {raw_warehouse_metering_history_table.uri} - A source table to accumulate the metering data
        - {metrics_warehouse_metering_table.uri} - A metrics table for the metering data with added statistics like SMA and STD
        - {feature_warehouse_metering_table.uri} - A feature table for the deseasonalized metering data
        """

with DAG(
    dag_id="data_preparation",
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
    catchup=False,
    max_active_runs=1,
    doc_md=doc_md,
    template_searchpath="/usr/local/airflow/include",
):

    @task(
        doc_md="""We expect to have metering data for all dates between dag_start_date
                  and the current execution. This task ensures the raw table has been
                  loaded for all past dates. Fail if any dates are missing so we
                  can manually backfill the missing dates."""
    )
    def validate_raw_metering_table(dag=None, data_interval_start=None):
        dag_start_date = dag.start_date.strftime("%Y-%m-%d")
        data_interval_start = data_interval_start.strftime("%Y-%m-%d")

        query = f"""
                WITH calc AS (
                    SELECT  COUNT(DISTINCT USAGE_DATE) AS LOADED_DATES,
                            DATEDIFF('days', '{dag_start_date}', '{data_interval_start}') AS TOTAL_DATES
                    FROM {raw_warehouse_metering_history_table.uri}
                    WHERE USAGE_DATE BETWEEN '{dag_start_date}' AND '{data_interval_start}'
                )
                SELECT (TOTAL_DATES - LOADED_DATES) AS MISSING_DATES
                FROM calc
                """

        missing_date_count = snowflake_hook.get_first(query)[0]

        if missing_date_count > 0:
            raise DataValidationFailed(
                f"{missing_date_count} missing dates found in table {raw_warehouse_metering_history_table.uri}. "
                f"Make sure the Data Ingestion Dag has run for all dates before attempting to run this Dag."
            )

    load_metrics_warehouse_metering_table = SQLExecuteQueryOperator(
        doc_md="""
            Task to perform Feature Engineering of the Warehouse Metering Table.
            We create a matrix of all warehouses and all dates, then join
            the warehouse metering data to it. We then compute the 30-day Simple Moving Average (SMA)
            and standard deviations (STD) for each metric.
            This will allow us to leverage trends in future versions.
            """,
        task_id="load_metrics_warehouse_metering_table",
        conn_id=snowflake_conn_id,
        outlets=metrics_warehouse_metering_table,
        sql="sql/data_preparation/metrics_warehouse_metering_table.sql",
        params={
            "table_name": metrics_warehouse_metering_table.uri,
            "raw_metering_table": raw_warehouse_metering_history_table.uri,
            "common_calendar_table": common_calendar_table.uri,
        },
    )

    @task(
        outlets=feature_warehouse_metering_table,
        doc_md="""Generate our Feature table by decomposing the seasonality of the metering data
                  into trend, seasonal, and residual components. We use STL decomposition for simplicity.""",
    )
    def load_feature_warehouse_metering_table(logical_date=None):
        ds = logical_date.strftime("%Y-%m-%d")

        decomposed_metering_dfs = []

        metering_df = snowflake_hook.get_pandas_df(
            sql=f"""
            SELECT WAREHOUSE_NAME, USAGE_DATE, CREDITS_USED
            FROM {metrics_warehouse_metering_table.uri}
            WHERE USAGE_DATE <= '{ds}'
            ORDER BY USAGE_DATE ASC;
            """,
        )

        # Decompose each warehouse separately
        for warehouse in metering_df["WAREHOUSE_NAME"].unique().tolist():
            warehouse_df = metering_df[metering_df["WAREHOUSE_NAME"] == warehouse]
            warehouse_df = warehouse_df[["USAGE_DATE", "CREDITS_USED"]]
            warehouse_df.set_index(["USAGE_DATE"], inplace=True)
            warehouse_df = warehouse_df.asfreq("D", fill_value=0)
            warehouse_df.sort_index(inplace=True)

            try:
                stl = seasonal_decompose(
                    x=warehouse_df,
                    model="additive",
                    extrapolate_trend="freq",
                )
            except ValueError:
                logging.warning(
                    f"Could not perform seasonal_decompose on {warehouse}. Skipping."
                )
                continue
            stl_df = pd.DataFrame(
                {
                    "TREND": stl.trend,
                    "SEASONAL": stl.seasonal,
                    "RESIDUAL": stl.resid,
                }
            )
            decomposed_df = pd.concat([warehouse_df, stl_df], axis=1)
            decomposed_df["WAREHOUSE_NAME"] = warehouse
            decomposed_df.reset_index(inplace=True)
            decomposed_metering_dfs.append(decomposed_df)

        if decomposed_metering_dfs == []:
            logging.warning("No decomposed data to write to feature_metering_table.")
            return

        decomposed_metering_df = pd.concat(decomposed_metering_dfs)

        decomposed_metering_df.to_sql(
            name=feature_warehouse_metering_table.uri.split(".")[-1].lower(),
            con=snowflake_hook.get_sqlalchemy_engine(),
            dtype={
                "USAGE_DATE": sqlalchemy.types.Date,
                "CREDITS_USED": sqlalchemy.types.Numeric(38, 9),
                "TREND": sqlalchemy.types.Float,
                "SEASONAL": sqlalchemy.types.Float,
                "RESIDUAL": sqlalchemy.types.Float,
                "WAREHOUSE_NAME": sqlalchemy.types.VARCHAR,
            },
            index=False,
            if_exists="replace",
        )

    (
        validate_raw_metering_table()
        >> load_metrics_warehouse_metering_table
        >> load_feature_warehouse_metering_table()
    )
