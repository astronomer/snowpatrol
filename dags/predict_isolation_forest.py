import os
import pickle
from tempfile import TemporaryDirectory

import pandas as pd
import wandb
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from statsmodels.tsa.seasonal import seasonal_decompose
from snowflake.connector.pandas_tools import pd_writer

wandb_project = os.getenv("WANDB_PROJECT")
wandb_entity = os.getenv("WANDB_ENTITY")

_SNOWFLAKE_CONN_ID = "snowflake_admin"
_SLACK_CONN_ID = "slack_api_alert"

alert_channel_name = "#snowstorm-alerts"

snowflake_hook = SnowflakeHook(_SNOWFLAKE_CONN_ID)

usage_table = Dataset(uri="DEMO.SNOWSTORM.USAGE_IN_CURRENCY_DAILY")

feature_table = Dataset(
    uri="DEMO.SNOWSTORM.USAGE_FEATURES",
    extra={"cutoff_date": "'2022-12-17'", "cost_categories": ["compute", "storage"]},
)

isolation_forest_model = Dataset(
    uri="isolation_forest_model",
    extra={"cost_models": ["total_usage", "compute", "storage"]},
)

usage_anomalies = Dataset(
    uri="USAGE_ANOMALIES",
    extra={"schema":"SNOWSTORM"}
)


@dag(
    default_args={},
    schedule=[feature_table],
    start_date=days_ago(2),
    is_paused_upon_creation=False,
)
def predict_isolation_forest():
    """
    This DAG performs predictions of anomalous activity in Snowflake usage.
    """

    @task()
    def predict(cost_category: str) -> pd.DataFrame:
        """ """
        model_name = f"isolation_forest_{cost_category}"

        wandb.login()

        with TemporaryDirectory() as model_dir:
            artifact = wandb.Api().artifact(
                name=f"{wandb_entity}/{wandb_project}/{model_name}:latest",
                type="model",
            )
            model_file_path = artifact.file(model_dir)
            with open(model_file_path, "rb") as mf:
                model = pickle.load(mf)

            usage_df = snowflake_hook.get_pandas_df(
                f"""SELECT DATE, {cost_category} 
                  FROM {feature_table.uri} 
                  ORDER BY DATE ASC;"""
            )
            # Snowflake returns columns in all caps, so we need to lowercase them
            usage_df.columns = usage_df.columns.str.lower()

            usage_df.date = pd.to_datetime(usage_df.date)
            usage_df.set_index("date", inplace=True)
            usage_df.fillna(value=0, inplace=True)

            stl = seasonal_decompose(
                usage_df[cost_category], model="additive", extrapolate_trend="freq"
            )

            usage_stationary = stl.resid.values.reshape(-1, 1)

            usage_df["scores"] = model.decision_function(usage_stationary)
            anomaly_threshold = artifact.metadata.get("anomaly_threshold")

            anomalies_df = usage_df.iloc[-5:].loc[
                (usage_df.scores <= anomaly_threshold)
                & (usage_df[cost_category] > usage_df[cost_category].mean()),
                [cost_category],
            ]

            return anomalies_df

    @task()
    def generate_report(anomaly_dfs: [pd.DataFrame]) -> str | None:
        anomalies_df = pd.concat(anomaly_dfs, axis=0).reset_index()

        report_dates = (
            anomalies_df.date.apply(lambda x: str(x.date())).unique().tolist()
        )

        if len(report_dates) > 0:
            usage_df = snowflake_hook.get_pandas_df(
                f"""SELECT ACCOUNT_NAME AS ACCOUNT, 
                            USAGE_DATE AS DATE, 
                            USAGE_TYPE_CLEAN AS TYPE, 
                            ROUND(USAGE_IN_CURRENCY, 2) AS USAGE 
                        FROM {usage_table.uri} 
                        WHERE DATE IN ({str(report_dates)[1:-1]})
                    ORDER BY DATE ASC, USAGE DESC;"""
            )

            snowflake_df = usage_df.reset_index(drop=True)
            conn = snowflake_hook.get_sqlalchemy_engine()
            snowflake_df.to_sql(usage_anomalies.uri,
                                schema=usage_anomalies.extra.get("schema"),
                                con=conn,
                                if_exists="append",
                                index=False,
                                index_label=None,
                                method=pd_writer)

            usage_md = usage_df[["DATE", "USAGE", "TYPE", "ACCOUNT"]].to_markdown()

            report = "```# Anomalous activity in Snowflake usage\n\n" + usage_md + "```"

            return report
        else:
            return None


    @task.branch()
    def check_notify(anomaly_dfs: [pd.DataFrame]):
        if len(pd.concat(anomaly_dfs, axis=0)) > 0:
            return ["send_alert"]

    anomaly_dfs = predict.expand(
        cost_category=feature_table.extra.get("cost_categories")
    )

    notification_check = check_notify(anomaly_dfs=anomaly_dfs)

    report = generate_report(anomaly_dfs=anomaly_dfs)

    send_alert = SlackAPIPostOperator(
        trigger_rule="none_failed",
        task_id="send_alert",
        channel=alert_channel_name,
        text=report,
        slack_conn_id=_SLACK_CONN_ID
    )

    notification_check >> send_alert


predict_isolation_forest()
