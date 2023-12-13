from datetime import datetime

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

_SNOWFLAKE_CONN_ID = "snowflake_admin"

usage_table = Dataset(uri="DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY")

feature_table = Dataset(
    uri="DEMO.SNOWSTORM.USAGE_FEATURES",
    extra={"cutoff_date": "'2022-12-17'", "cost_categories": ["compute", "storage"]},
)


@dag(
    default_args={},
    schedule=[usage_table],
    start_date=datetime(2022, 12, 17),
    is_paused_upon_creation=False,
)
def feature_engineering():
    """
    This DAG performs Feature engineering of daily data from sources into tables for upstream ML DAGs.
    Usage Data is aggregated by date and by type, then merged into the feature table.
    """

    @task(outlets=feature_table)
    def build_features(**kwargs):
        snowflake_hook = SnowflakeHook(_SNOWFLAKE_CONN_ID)
        ts = str(kwargs["logical_date"])
        ds = kwargs["logical_date"].strftime("%Y-%m-%d")
        snowflake_hook.run(
            f"""
            MERGE INTO {feature_table.uri} AS target
            USING (
                SELECT all_data.USAGE_DATE AS DATE,
                TOTAL_USAGE,
                COMPUTE,
                STORAGE,
                TO_TIMESTAMP_NTZ('{ ts }') AS UPDATED_AT
                FROM (
                    SELECT *
                    FROM (
                        SELECT USAGE_DATE, USAGE, USAGE_TYPE_CLEAN
                        FROM DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY
                        WHERE USAGE_TYPE_CLEAN IN ('compute', 'storage')
                    )
                    PIVOT(SUM(USAGE) FOR USAGE_TYPE_CLEAN IN ('compute', 'storage'))
                    AS p(USAGE_DATE, COMPUTE, STORAGE)
                ) AS category_data
                JOIN (
                    SELECT USAGE_DATE, SUM(USAGE_IN_CURRENCY) AS TOTAL_USAGE
                    FROM (
                        SELECT USAGE_DATE, USAGE_IN_CURRENCY
                        FROM DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY
                        )
                    GROUP BY USAGE_DATE
                ) AS all_data
                ON category_data.USAGE_DATE = all_data.USAGE_DATE
                WHERE all_data.USAGE_DATE = '{ ds }'
            ) AS source
            ON  source.DATE = target.DATE
            WHEN MATCHED THEN
                UPDATE SET
                    target.DATE = source.DATE,
                    target.TOTAL_USAGE = source.TOTAL_USAGE,
                    target.COMPUTE = source.COMPUTE,
                    target.STORAGE = source.STORAGE,
                    target.UPDATED_AT = source.UPDATED_AT                    
            WHEN NOT MATCHED THEN
                INSERT (
                    DATE,
                    TOTAL_USAGE,
                    COMPUTE,
                    STORAGE,
                    UPDATED_AT
                ) VALUES (
                    source.DATE,
                    source.TOTAL_USAGE,
                    source.COMPUTE,
                    source.STORAGE,
                    source.UPDATED_AT
                );
            """
        )

    build_features()


feature_engineering()
