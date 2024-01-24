import plotly.express as px
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Snowflake Configuration
snowflake_conn_id = "snowflake_admin"
snowflake_hook = SnowflakeHook(snowflake_conn_id)


def get_anomaly_chart():
    anomaly_query = """SELECT "WAREHOUSE_NAME",
                        TO_DATE("USAGE_DATE") AS "USAGE_DATE",
                        ROUND("CREDITS_USED", 2) AS "CREDITS_USED",
                        ROUND("TREND", 2) AS "TREND",
                        ROUND("SEASONAL", 2) AS "SEASONAL",
                        ROUND("RESIDUAL", 2) AS "RESIDUAL",
                        ROUND("SCORE", 2) AS "SCORE"
                    FROM DEMO.SNOWSTORM.MODEL_OUTPUT_ANOMALIES
                    ORDER BY WAREHOUSE_NAME, USAGE_DATE"""

    metering_query = """SELECT USAGE_DATE,
                         WAREHOUSE_NAME,
                         CREDITS_USED,
                         CREDITS_USED_COMPUTE,
                         CREDITS_USED_CLOUD_SERVICES
                         FROM DEMO.SNOWSTORM.METRICS_WAREHOUSE_METERING_HISTORY
                         ORDER BY WAREHOUSE_NAME, USAGE_DATE"""

    anomalies_df = snowflake_hook.get_pandas_df(anomaly_query)
    metering_df = snowflake_hook.get_pandas_df(metering_query)
    # Plot the line chart
    fig = px.line(
        metering_df,
        x="USAGE_DATE",
        y="CREDITS_USED",
        color="WAREHOUSE_NAME",
        title="Credits Used Over Time",
        labels={"CREDITS_USED": "Credits Used", "Date_Column": "Date"},
    )

    # Highlight data points above the compute_threshold in a different color
    if not anomalies_df.empty:
        scatter_trace = px.scatter(
            anomalies_df,
            x="USAGE_DATE",
            y="CREDITS_USED",
            color_discrete_sequence=["black"],
        )
        fig.add_trace(scatter_trace.data[0])
    return fig.to_json()
