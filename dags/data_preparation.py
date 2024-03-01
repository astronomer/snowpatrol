import logging
from datetime import timedelta

import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from statsmodels.tsa.seasonal import seasonal_decompose
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta
from include.datasets import (common_calendar_table, feature_metering_table,
                              metrics_metering_table, raw_metering_table,
                              source_metering_table)
from include.exceptions import DataValidationFailed

# Snowflake Configuration
snowflake_conn_id = "snowflake_admin"
snowflake_hook = SnowflakeHook(snowflake_conn_id)

# Slack Configuration
slack_conn_id = "slack_alert"
slack_channel = "#snowstorm-alerts"

doc_md = f"""
        # Data Preparation
        This DAG performs the data preparation for Snowflake's Metering view. We use it
        to populate feature tables for ML DAGs. Corrections can be made to this view 
        up to 24 hours later. Only the last 365 days are kept. We perform a MERGE operation
        to ensure new data is inserted and past history is updated if changes happen for past dates.

        #### Tables
        - [{source_metering_table.uri}](https://docs.snowflake.com/en/sql-reference/organization-usage/warehouse_metering_history)
        - {common_calendar_table.uri} - A simple calendar table populated for 5 years starting on 2023-01-01
        - {raw_metering_table.uri} - A source table to accumulate the metering data
        - {metrics_metering_table.uri} - A metrics table for the metering data with added statistics like SMA and STD
        - {feature_metering_table.uri} - A feature table for the seasonal decomposition of the metering data
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
    start_date=timezone.utcnow()-relativedelta(years=+1),
    catchup=False,
    max_active_runs=1,
    doc_md=doc_md,
):
    load_raw_metering_table = SQLExecuteQueryOperator(
        # TODO: This should be an incremental load with the day's data only
        doc_md="""
            Task to persist the data from the view
            SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY.
            This view keeps the last 365 days of data so we need to persist it ourselves.
            We use a MERGE operation to ensure past history is updated if changes occured.
            """,
        task_id="load_metering_table",
        conn_id=snowflake_conn_id,
        outlets=raw_metering_table,
        sql=f"""
            MERGE INTO {raw_metering_table.uri} AS target
            USING (
                SELECT ACCOUNT_NAME,
                       SERVICE_TYPE,
                       TO_DATE(END_TIME) AS USAGE_DATE,
                       WAREHOUSE_NAME,
                       SUM(CREDITS_USED) AS CREDITS_USED,
                       SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE,
                       SUM(CREDITS_USED_CLOUD_SERVICES) AS CREDITS_USED_CLOUD_SERVICES,
                       SYSDATE() AS UPDATED_AT
                FROM {source_metering_table.uri}
                WHERE   WAREHOUSE_ID > 0  -- Skip pseudo-VWs such as "CLOUD_SERVICES_ONLY"
                AND     USAGE_DATE BETWEEN DATEADD(DAY, -2, '{{{{ ds }}}}') AND '{{{{ ds }}}}'
                AND     ACCOUNT_NAME = 'GP21411' -- Astronomer's main Snowflake account
                GROUP BY ACCOUNT_NAME,
                         SERVICE_TYPE,
                         USAGE_DATE,
                         WAREHOUSE_NAME
            ) AS source
            ON  source.ACCOUNT_NAME     = target.ACCOUNT_NAME
            AND source.SERVICE_TYPE     = target.SERVICE_TYPE
            AND source.USAGE_DATE       = target.USAGE_DATE
            AND source.WAREHOUSE_NAME   = target.WAREHOUSE_NAME
            WHEN MATCHED THEN
                UPDATE SET
                    target.ACCOUNT_NAME                 = source.ACCOUNT_NAME,
                    target.SERVICE_TYPE                 = source.SERVICE_TYPE,
                    target.USAGE_DATE                   = source.USAGE_DATE,
                    target.WAREHOUSE_NAME               = source.WAREHOUSE_NAME,
                    target.CREDITS_USED                 = source.CREDITS_USED,
                    target.CREDITS_USED_COMPUTE         = source.CREDITS_USED_COMPUTE,
                    target.CREDITS_USED_CLOUD_SERVICES  = source.CREDITS_USED_CLOUD_SERVICES,
                    target.UPDATED_AT                   = source.UPDATED_AT
            WHEN NOT MATCHED THEN
                INSERT (
                    ACCOUNT_NAME,
                    SERVICE_TYPE,
                    USAGE_DATE,
                    WAREHOUSE_NAME,
                    CREDITS_USED,
                    CREDITS_USED_COMPUTE,
                    CREDITS_USED_CLOUD_SERVICES,
                    UPDATED_AT
                ) VALUES (
                    source.ACCOUNT_NAME,
                    source.SERVICE_TYPE,
                    source.USAGE_DATE,
                    source.WAREHOUSE_NAME,
                    source.CREDITS_USED,
                    source.CREDITS_USED_COMPUTE,
                    source.CREDITS_USED_CLOUD_SERVICES,
                    source.UPDATED_AT
                );
            """,
    )

    load_common_calendar_table = SQLExecuteQueryOperator(
        doc_md="""
                Task to create a calendar table with 5 years starting on dag_start_date
                """,
        task_id="load_calendar_table",
        conn_id=snowflake_conn_id,
        outlets=common_calendar_table,
        sql=f"""
                CREATE OR REPLACE TABLE {common_calendar_table.uri} (
                    USAGE_DATE      DATE        NOT NULL,
                    YEAR            SMALLINT    NOT NULL,
                    MONTH           SMALLINT    NOT NULL,
                    MONTH_NAME      CHAR(3)     NOT NULL,
                    DAY_OF_MON      SMALLINT    NOT NULL,
                    DAY_OF_WEEK     VARCHAR(9)  NOT NULL,
                    WEEK_OF_YEAR    SMALLINT    NOT NULL,
                    DAY_OF_YEAR     SMALLINT    NOT NULL
                )
                AS
                WITH dates AS (
                    SELECT '{{{{ dag.start_date }}}}'::DATE -1 +
                      ROW_NUMBER() OVER(ORDER BY 0) AS USAGE_DATE
                    FROM TABLE(GENERATOR(ROWCOUNT => 1826)) -- 5 years
                )
                SELECT  USAGE_DATE,
                        YEAR(USAGE_DATE),
                        MONTH(USAGE_DATE),
                        MONTHNAME(USAGE_DATE),
                        DAY(USAGE_DATE),
                        DAYOFWEEK(USAGE_DATE),
                        WEEKOFYEAR(USAGE_DATE),
                        DAYOFYEAR(USAGE_DATE)
                FROM dates;
            """,
    )

    @task(
        doc_md="""We expect to have metering data for all dates between dag_start_date 
                  and the current execution. This task ensures the raw table has been 
                  loaded for all past dates. Fail if any dates are missing so we 
                  can manually backfill the missing dates."""
    )
    def validate_raw_metering_table(dag=None, data_interval_start=None):
        # TODO: Improve this to return a list of missing dates in the error message
        dag_start_date = dag.start_date.strftime("%Y-%m-%d")
        data_interval_start = data_interval_start.strftime("%Y-%m-%d")

        query = f"""
                WITH calc AS (
                    SELECT  COUNT(DISTINCT USAGE_DATE) AS LOADED_DATES,
                            DATEDIFF('days', '{dag_start_date}', '{data_interval_start}') AS TOTAL_DATES
                    FROM {raw_metering_table.uri}
                    WHERE USAGE_DATE BETWEEN '{dag_start_date}' AND '{data_interval_start}'
                )
                SELECT (TOTAL_DATES - LOADED_DATES) AS MISSING_DATES
                FROM calc
                """

        missing_date_count = snowflake_hook.get_first(query)[0]

        if missing_date_count > 0:
            raise DataValidationFailed(
                f"{missing_date_count} missing dates found in table {raw_metering_table.uri}"
            )

    load_metrics_metering_table = SQLExecuteQueryOperator(
        doc_md="""
            Task to perform Feature Engineering of the Metering Table.
            We create a matrix of all warehouses and all dates, then join
            the metering data to it. We then compute the 30-day Simple Moving Average (SMA)
            and standard deviations (STD) for each metric.
            This will allow us to plot Bollinger Bands later on.
            """,
        task_id="load_metrics_metering_table",
        conn_id=snowflake_conn_id,
        outlets=metrics_metering_table,
        sql=f"""
            CREATE OR REPLACE TABLE {metrics_metering_table.uri} AS
            /* Create a virtual table of all warehouses */
            WITH virtual_warehouses AS (
                SELECT DISTINCT WAREHOUSE_NAME
                FROM {raw_metering_table.uri}
                WHERE WAREHOUSE_NAME IS NOT NULL
                ORDER BY WAREHOUSE_NAME
            ), 
            /* Create a cross product of all warehouses and all dates */
            cross_product AS (
                SELECT  WAREHOUSE_NAME,
                        USAGE_DATE
                FROM virtual_warehouses
                CROSS JOIN (
                    SELECT USAGE_DATE
                    FROM {common_calendar_table.uri}
                    WHERE USAGE_DATE <= '{{{{ ds }}}}'
                ) AS calendar
            ), 
            /* Join the metering data to the cross product to have a complete matrix */
            metering AS (
                SELECT cross_product.WAREHOUSE_NAME,
                       cross_product.USAGE_DATE,
                       COALESCE(SUM(metering.CREDITS_USED), 0) AS CREDITS_USED,
                       COALESCE(SUM(metering.CREDITS_USED_COMPUTE), 0) AS CREDITS_USED_COMPUTE,
                       COALESCE(SUM(metering.CREDITS_USED_CLOUD_SERVICES), 0) AS CREDITS_USED_CLOUD_SERVICES
                FROM cross_product
                LEFT JOIN {raw_metering_table.uri} AS metering
                ON  cross_product.USAGE_DATE        = metering.USAGE_DATE
                AND cross_product.WAREHOUSE_NAME    = metering.WAREHOUSE_NAME
                GROUP BY
                    cross_product.WAREHOUSE_NAME,
                    cross_product.USAGE_DATE
            )
            /* Compute Simple Moving averages and Standard Deviations */
            SELECT  WAREHOUSE_NAME,
                    USAGE_DATE,
                    CREDITS_USED,
                    CREDITS_USED_COMPUTE,
                    CREDITS_USED_CLOUD_SERVICES,
                    AVG(CREDITS_USED) OVER (
                        PARTITION BY WAREHOUSE_NAME 
                        ORDER BY USAGE_DATE
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS CREDITS_USED_SMA30,
                    AVG(CREDITS_USED_COMPUTE) OVER (
                        PARTITION BY WAREHOUSE_NAME 
                        ORDER BY USAGE_DATE 
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS CREDITS_USED_COMPUTE_SMA30,
                    AVG(CREDITS_USED_CLOUD_SERVICES) OVER (
                        PARTITION BY WAREHOUSE_NAME 
                        ORDER BY USAGE_DATE 
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS CREDITS_USED_CLOUD_SERVICES_SMA30,
                    STDDEV(CREDITS_USED) OVER (
                        PARTITION BY WAREHOUSE_NAME 
                        ORDER BY USAGE_DATE 
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS CREDITS_USED_STD30,
                    STDDEV(CREDITS_USED_COMPUTE) OVER (
                        PARTITION BY WAREHOUSE_NAME 
                        ORDER BY USAGE_DATE 
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS CREDITS_USED_COMPUTE_STD30,
                    STDDEV(CREDITS_USED_CLOUD_SERVICES) OVER (
                        PARTITION BY WAREHOUSE_NAME 
                        ORDER BY USAGE_DATE 
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS CREDITS_USED_CLOUD_SERVICES_STD30
            FROM metering
            ORDER BY WAREHOUSE_NAME,
                     USAGE_DATE;
        """,
    )

    @task(
        outlets=feature_metering_table,
        doc_md="""Generate our Feature table by decomposing the seasonality of the metering data
                  into trend, seasonal, and residual components. We use STL decomposition for simplicity.""",
    )
    def load_feature_metering_table(logical_date=None):
        # TODO: Version the Feature Metering Table
        ds = logical_date.strftime("%Y-%m-%d")

        decomposed_metering_dfs = []

        # Consider doing this with Dynamic Task Mapping too if computation time is too long
        metering_df = snowflake_hook.get_pandas_df(
            sql=f"""
            SELECT WAREHOUSE_NAME, USAGE_DATE, CREDITS_USED
            FROM {metrics_metering_table.uri}
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
            name=feature_metering_table.uri.split(".")[-1].lower(),
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
        [load_raw_metering_table, load_common_calendar_table]
        >> validate_raw_metering_table()
        >> load_metrics_metering_table
        >> load_feature_metering_table()
    )
