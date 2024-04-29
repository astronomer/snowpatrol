from __future__ import annotations

import logging
import os
import pickle
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

import plotly.express as px
import wandb
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from sklearn.ensemble import IsolationForest

from include.datasets import feature_metering_table, isolation_forest_model

# Snowflake Configuration
snowflake_conn_id = "snowflake_admin"
snowflake_hook = SnowflakeHook(snowflake_conn_id)

# Slack Configuration
slack_conn_id = "slack_alert"
slack_channel = "#snowstorm-alerts"

# Weights and Biases Configuration
wandb_project = os.getenv("WANDB_PROJECT")
wandb_entity = os.getenv("WANDB_ENTITY")

# Model Configuration
model_config = {
    "architecture": "Isolation Forest",
    "dataset": feature_metering_table.uri,
    "threshold_cutoff": 3,  # we will assume X STDDev from the mean as anomalous
}

doc_md = """
    This DAG performs training of isolation forest models for each Snowflake Virtual Warehouses.
    A separate model is trained for each warehouse using Dynamic Task Mapping.

     #### Tables
        - {feature_metering_table.uri} - A feature table for the seasonal decomposition of the metering data
    """

with DAG(
    dag_id="train_isolation_forest",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": send_slack_notification(
            slack_conn_id="slack_alert",
            text="The task {{ ti.task_id }} failed. Check the logs.",
            channel=slack_channel,
        ),
    },
    schedule=[feature_metering_table],
    start_date=datetime(2023, 1, 1),
    is_paused_upon_creation=False,
    catchup=False,
    doc_md=doc_md,
):

    @task(
        doc_md="Get a list of warehouses and use it to expand the training task with Dynamic Task Mapping."
    )
    def list_warehouses() -> list[str]:
        df = snowflake_hook.get_pandas_df(
            f"""SELECT DISTINCT WAREHOUSE_NAME
                    FROM {feature_metering_table.uri}
                    ORDER BY WAREHOUSE_NAME ASC;"""
        )
        return df["WAREHOUSE_NAME"].tolist()

    @task(
        outlets=[isolation_forest_model],
        doc_md="Train an isolation forest model for a given warehouse.",
        map_index_template="{{ model_name }}",
    )
    def train_isolation_forest(warehouse: str, data_interval_start=None, run_id=None):
        # We use the Airflow Run ID to group experiments for all warehouses together
        group_name = run_id.replace(":", "")
        model_name = f"isolation_forest_{warehouse}"

        # Add model_name to current context to use in map_index_template
        context = get_current_context()
        context["model_name"] = warehouse

        wandb.login()

        with (
            TemporaryDirectory() as model_dir,
            wandb.init(
                project=wandb_project,
                entity=wandb_entity,
                dir=model_dir,
                name=model_name,
                group=group_name,
                job_type="train_isolation_forest",
                resume="allow",
                force=True,
                config=model_config,
            ),
        ):
            df = snowflake_hook.get_pandas_df(
                sql=f"""SELECT USAGE_DATE,
                               CREDITS_USED,
                               RESIDUAL
                      FROM {feature_metering_table.uri}
                      WHERE WAREHOUSE_NAME = '{warehouse}'
                      AND   USAGE_DATE < '{data_interval_start}'
                      ORDER BY USAGE_DATE ASC;
                      """,
            )

            df.set_index("USAGE_DATE", inplace=True)
            df = df.asfreq("D", fill_value=0)

            # Normalize the residuals to use for anomaly detection
            stationary = df["RESIDUAL"].values.reshape(-1, 1)

            model = IsolationForest().fit(stationary)

            df["SCORES"] = model.decision_function(stationary)

            mean_scores = df["SCORES"].mean()
            std_scores = df["SCORES"].std()

            anomaly_threshold = mean_scores - (
                model_config["threshold_cutoff"] * std_scores
            )
            df["THRESHOLD"] = anomaly_threshold

            anomalies_df = df.loc[
                (df["SCORES"] <= df["THRESHOLD"])
                & (df["CREDITS_USED"] > df["CREDITS_USED"].mean())
            ]
            logging.info(anomalies_df)

            # save the model artifact as a pickle file
            with open(f"{model_dir}/{model_name}.pkl", "wb") as model_file:
                pickle.dump(model, model_file)

            # Plot a line chart of the used credits and highlight anomalies
            fig = px.line(
                df,
                x=df.index,
                y="CREDITS_USED",
                color_discrete_sequence=["black"],
                title="Credits Used Over Time",
                labels={"CREDITS_USED": "Credits Used", "Date_Column": "Date"},
            )
            # Highlight data points above the compute_threshold in a different color
            if not anomalies_df.empty:
                scatter_trace = px.scatter(
                    anomalies_df,
                    x=anomalies_df.index,
                    y="CREDITS_USED",
                    color_discrete_sequence=["red"],
                )
                fig.add_trace(scatter_trace.data[0])
            # Save the plot
            fig.write_image(
                f"{model_dir}/{warehouse}_anomalies.png",
                format("png"),
            )

            # Upload the model artifact and the charts to Weights and Biases
            artifact = wandb.Artifact(
                model_name,
                type="model",
                metadata={"anomaly_threshold": anomaly_threshold},
            )
            artifact.add_file(local_path=f"{model_dir}/{model_name}.pkl")
            wandb.log_artifact(artifact)
            wandb.run.link_artifact(
                artifact, f"{wandb_entity}/{wandb_project}/{model_name}:latest"
            )

            wandb.log(
                {"anomalies": wandb.Image(f"{model_dir}/{warehouse}_anomalies.png")}
            )
        return model_name

    warehouses = list_warehouses()
    train_isolation_forest.expand(warehouse=warehouses)
