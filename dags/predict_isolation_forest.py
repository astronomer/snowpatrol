import os
import pickle
from datetime import timedelta
from tempfile import TemporaryDirectory

import pandas as pd
import wandb
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from dateutil.relativedelta import relativedelta

from include.datasets import (
    feature_warehouse_metering_table,
    isolation_forest_model,
    model_output_anomalies_table,
    raw_warehouse_metering_history_table,
)

# Weights and Biases Configuration
wandb_project = os.getenv("WANDB_PROJECT")
wandb_entity = os.getenv("WANDB_ENTITY")

# Snowflake Configuration
snowflake_conn_id = "snowflake_conn"
snowflake_hook = SnowflakeHook(snowflake_conn_id)

# Slack Configuration
slack_conn_id = "slack_alert"
slack_channel = "#snowstorm-alerts"


doc_md = """
         This DAG performs predictions of anomalous activity in Snowflake usage.
         Send a Slack alert if anomalies are detected.
         """


with DAG(
    dag_id="predict_isolation_forest",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": True,
        "on_failure_callback": send_slack_notification(
            slack_conn_id="slack_alert",
            text="The task {{ ti.task_id }} failed. Check the logs.",
            channel=slack_channel,
        ),
    },
    schedule=[
        isolation_forest_model,
    ],
    start_date=timezone.utcnow() - relativedelta(years=+1),
    catchup=False,
    doc_md=doc_md,
):

    @task(
        doc_md="Get a list of warehouses and use it to expand the predict task with Dynamic Task Mapping."
    )
    def list_warehouses() -> list[str]:
        df = snowflake_hook.get_pandas_df(
            f"""SELECT DISTINCT WAREHOUSE_NAME
                FROM {feature_warehouse_metering_table.uri}
                ORDER BY WAREHOUSE_NAME ASC;"""
        )
        return df["WAREHOUSE_NAME"].tolist()

    @task(
        doc_md="Predict anomalous activity in Snowflake usage.",
        outlets=[model_output_anomalies_table],
        map_index_template="{{ warehouse }}",
    )
    def predict_metering_anomalies(
        warehouse, data_interval_start=None, logical_date=None
    ) -> pd.DataFrame:
        model_name = f"isolation_forest_{warehouse}"

        # Add model_name to current context to use in map_index_template
        context = get_current_context()
        context["model_name"] = warehouse

        wandb.login()

        with TemporaryDirectory() as model_dir:
            artifact = wandb.Api().artifact(
                name=f"{wandb_entity}/{wandb_project}/{model_name}:latest",
                type="model",
            )
            model_file_path = artifact.file(model_dir)
            with open(model_file_path, "rb") as mf:
                model = pickle.load(mf)

            metering_df = snowflake_hook.get_pandas_df(
                f"""SELECT WAREHOUSE_NAME,
                           USAGE_DATE,
                           CREDITS_USED,
                           TREND,
                           SEASONAL,
                           RESIDUAL
                    FROM {feature_warehouse_metering_table.uri}
                    WHERE   WAREHOUSE_NAME = '{warehouse}'
                    AND     USAGE_DATE BETWEEN DATEADD(DAY, -7, '{data_interval_start}') AND '{data_interval_start}'
                    ORDER BY USAGE_DATE ASC;"""
            )

            usage_stationary = metering_df["RESIDUAL"].values.reshape(-1, 1)
            metering_df["SCORE"] = model.decision_function(usage_stationary)
            anomaly_threshold = artifact.metadata.get("anomaly_threshold")
            anomalies_df = metering_df.loc[(metering_df["SCORE"] <= anomaly_threshold)]
            anomalies_df["PREDICTION_DATETIME"] = logical_date

            # Write anomalies to the model output table
            anomalies_df.to_sql(
                name=model_output_anomalies_table.uri.split(".")[-1].lower(),
                con=snowflake_hook.get_sqlalchemy_engine(),
                if_exists="append",
                index=False,
                index_label=None,
            )

            return anomalies_df

    @task(doc_md="Generate a report of anomalous activity in Snowflake usage.")
    def generate_report(anomaly_dfs: [pd.DataFrame]) -> str | None:
        anomalies_df = pd.concat(anomaly_dfs, axis=0).reset_index()

        if len(anomalies_df) > 0:
            min_date = anomalies_df["USAGE_DATE"].min()
            max_date = anomalies_df["USAGE_DATE"].max()
            warehouses = anomalies_df["WAREHOUSE_NAME"].unique().tolist()

            usage_df = snowflake_hook.get_pandas_df(
                f"""SELECT WAREHOUSE_NAME,
                           USAGE_DATE,
                           CREDITS_USED,
                           CREDITS_USED_COMPUTE,
                           CREDITS_USED_CLOUD_SERVICES
                     FROM {raw_warehouse_metering_history_table.uri}
                     WHERE USAGE_DATE BETWEEN DATEADD(DAY, -7, '{min_date}') AND DATEADD(DAY, 7, '{max_date}')
                     AND WAREHOUSE_NAME IN ({','.join([f"'{w}'" for w in warehouses])})
                     ORDER BY WAREHOUSE_NAME, USAGE_DATE ASC;
                """
            )
            usage_df["USAGE_DATE"] = pd.to_datetime(usage_df["USAGE_DATE"])
            anomalies_df["USAGE_DATE"] = pd.to_datetime(anomalies_df["USAGE_DATE"])

            report_df = usage_df.merge(
                right=anomalies_df[["WAREHOUSE_NAME", "USAGE_DATE", "SCORE"]],
                how="left",
                left_on=["USAGE_DATE", "WAREHOUSE_NAME"],
                right_on=["USAGE_DATE", "WAREHOUSE_NAME"],
            )
            report_df["IS_ANOMALY"] = report_df["SCORE"].notnull()

            report_md = report_df.to_markdown()

            report = (
                "```# Anomalous activity in Snowflake usage\n\n" + report_md + "```"
            )

            return report
        else:
            return None

    @task.branch()
    def check_notify(anomaly_dfs: [pd.DataFrame]):
        if len(pd.concat(anomaly_dfs, axis=0)) > 0:
            return ["send_alert"]

    anomaly_dfs = predict_metering_anomalies.expand(warehouse=list_warehouses())

    notification_check = check_notify(anomaly_dfs=anomaly_dfs)
    report = generate_report(anomaly_dfs=anomaly_dfs)

    send_report = SlackAPIPostOperator(
        task_id="send_alert",
        channel=slack_channel,
        text=report,
        slack_conn_id=slack_conn_id,
    )

    notification_check >> send_report
