from airflow.decorators import dag, task
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

_SNOWFLAKE_CONN_ID = "snowflake_ro"

usage_table = Dataset(uri="USAGE_IN_CURRENCY_DAILY")

feature_table = Dataset(uri="USAGE_FEATURES", 
                        extra={
                            "cutoff_date": "'2022-11-15'",
                            "cost_categories": ["compute", "storage"]
                            })

@dag(
    default_args={},
    schedule=[usage_table],
    start_date=days_ago(2),
    is_paused_upon_creation=False,
)
def feature_engineering():
    """
    This DAG performs feature engineering
    """

    @task(outlets=feature_table)
    def build_features():
        """



        

        """

        cost_categories = feature_table.extra.get("cost_categories")
        cutoff_date = feature_table.extra.get("cutoff_date")

        snowflake_hook = SnowflakeHook(_SNOWFLAKE_CONN_ID)

        snowflake_hook.run(f"""
            CREATE OR REPLACE TABLE {feature_table.uri}(
                "date" DATE, 
                {",".join([f'"{cat}" NUMBER(38,6)' for cat in cost_categories])}
            )  AS SELECT *
                  FROM ( 
                    SELECT USAGE_DATE, USAGE, USAGE_TYPE 
                    FROM {usage_table.uri} )
                  PIVOT(SUM(USAGE) FOR USAGE_TYPE in ({str(cost_categories)[1:-1]})) as p
                  (date, {", ".join(cost_categories)})
                  WHERE ((date >= DATE {cutoff_date}) 
                      AND (date < CURRENT_DATE())) 
                  ORDER BY date
            """)

    build_features()

feature_engineering()