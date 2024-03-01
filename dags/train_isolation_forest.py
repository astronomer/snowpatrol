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
from airflow.models.param import Param
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
    "threshold_cutoff": 3,  # we will assume x STDDev from the mean as anomalous
}

doc_md = """
    This DAG performs training of isolation forest models for each Snowflake
    Virtual Warehouses. We validate that the feature table has been loaded
    for all past dates before training. A separate model is trained for
    each warehouse using Dynamic Task Mapping if data drift is detected.
    Setting the force_retrain parameter to True will force retraining of all models.
    We force retraining of models if we don't have enough data to detect drift.

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
                text="The task {{ ti.task_id }} failed. Check the logs.",  # TODO: Add link to logs
                channel=slack_channel,
            ),
        },
        schedule=[feature_metering_table],
        start_date=datetime(2023, 1, 1),
        is_paused_upon_creation=False,
        catchup=False,
        doc_md=doc_md,
        params={
            "force_retrain": Param(
                title="Force retraining",
                type="boolean",
                description="""By default, only the drifted models will be retrained.
                           Setting this to True will force retraining of all models.""",
                default=True,
            ),
        },
):
    @task.venv(
        "evidently_venv",
        doc_md="""Detect drift in the metering feature table for all warehouses.
                  Evidently has conflicting dependencies with Airflow and
                  must run in it's own Python Virtual Environment.
                  We use [astro-provider-venv](https://github.com/astronomer/astro-provider-venv)
                  to manage the external python virtual environment.""",
    )
    def detect_drift_metering(
            force_retrain: bool,
            snowflake_conn_config: dict,
            feature_metering_table: str,
            data_interval_start=None,
    ):
        import logging

        import pandas as pd
        from evidently.test_suite import TestSuite
        from evidently.tests import TestAllFeaturesValueDrift
        from snowflake import connector

        # Query the metering feature table
        conn = connector.connect(**snowflake_conn_config)
        cur = conn.cursor()

        query = f"""SELECT USAGE_DATE,
                           WAREHOUSE_NAME,
                           CREDITS_USED
                    FROM {feature_metering_table}
                    where USAGE_DATE < '{data_interval_start}'
                    ORDER BY USAGE_DATE ASC;
                    """
        cur.execute(query)
        metering_df = cur.fetch_pandas_all()

        all_warehouses = metering_df["WAREHOUSE_NAME"].unique().tolist()
        all_dates = metering_df["USAGE_DATE"].nunique()

        # Force retraining for all warehouses if we don't have 30 days of data
        if all_dates < 30:
            logging.info(
                "Not enough data do detect drift. Forcing retraining of all models."
            )
            return all_warehouses

        # If we have sufficient data, we retrain only for warehouses that have drifted.

        # To test drift we split the data into two sets: reference and current.
        # Current is the last 7 days of data. Reference is remaining data.
        # Drift is detected by comparing the two distributions using the KS test.

        # Pivot the data so that each warehouse is a column
        metering_df = metering_df.pivot(
            index="USAGE_DATE", columns="WAREHOUSE_NAME", values="CREDITS_USED"
        )
        metering_df.reset_index(inplace=True)

        cutoff_date = (metering_df["USAGE_DATE"].max() - pd.DateOffset(days=7)).date()

        reference_df = metering_df[metering_df["USAGE_DATE"] < cutoff_date]
        current_df = metering_df[metering_df["USAGE_DATE"] >= cutoff_date]

        # Run the KS test for all warehouses using Evidently
        suite = TestSuite(tests=[TestAllFeaturesValueDrift(stattest="ks")])
        suite.run(reference_data=reference_df, current_data=current_df)

        results_df = pd.DataFrame(
            [x.get("parameters") for x in suite.as_dict().get("tests")]
        )
        # TODO: Check where the null Warehouse name is coming from
        results_df = results_df.dropna().sort_values(["column_name"])
        logging.info(results_df.to_string())

        drifted_warehouses = results_df[results_df["detected"] == True][
            "column_name"
        ].values.tolist()

        # We put the logic here so that we still log the results
        # of the drift detection even if we override it.
        if force_retrain:
            logging.info("Forcing retraining of all models.")
            return all_warehouses
        else:
            return drifted_warehouses


    @task(
        outlets=[isolation_forest_model],
        doc_md="Train an isolation forest model for a given warehouse.",
    )
    def train_isolation_forest(warehouse: str, data_interval_start=None, run_id=None):
        # We use the Airflow Run ID to group experiments for all warehouses together
        group_name = run_id.replace(":", "")
        model_name = f"isolation_forest_{warehouse}"

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


    drifted_warehouses = detect_drift_metering(
        force_retrain="{{ params.force_retrain }}",
        snowflake_conn_config=snowflake_hook._get_conn_params(),  # TODO: Improve this to use public methods
        feature_metering_table=feature_metering_table.uri,
    )
    train_isolation_forest.expand(warehouse=drifted_warehouses)