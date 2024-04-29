### Modeling

Snowflake users incur costs for compute, storage, marketplace, and various other services. Of these, compute is the most
significant. Data exploration and experimentation were performed (see [notebook](notebooks/snowpatrol.ipynb)) to evaluate
the ability to identify anomalies for all warehouses.
While storage usage can increase quickly the relative cost is not significant (<1% of total billing).
Production models, therefore, will monitor compute usage specifically.

<p align="center">
  <img src="docs/images/transforming_decompose.png" width="400" align="left"/>
</p>

Without specific labeled data we will use an unsupervised approach with
an [Isolation Forest](https://en.wikipedia.org/wiki/Isolation_forest) model. Separate models
will be trained for each warehouse using dynamic tasks. Because the usage demonstrates both seasonality and
increasing trend, the model will be trained on decomposed residuals factoring out both seasonality and trend.
<br clear="left"/>

### DAGs

This project separates all the steps of the ML pipeline into 3 different Dags.
`Data Preparation` handles data extraction, transformation, and feature engineering.
`Train Isolation Forest` handles model training
`Predict Isolation Forest` handles predictions, monitoring and alerting.

<p align="center"><img src="docs/images/dags.png" width="800"/></p>

#### Data Preparation

Snowflake performs nightly updates to the metering statistics and makes the data available in
the `WAREHOUSE_METERING_HISTORY` table.
While we could use the tables directly, the data is truncated daily and only the last 365 days are kept.
The data preparation DAG extracts organization-level metering data, cleans it up and accumulates it so we have a full
history. To keep things simple, data validation and feature engineering are done as part of the same DAG.

Data validation is performed after the raw data is sourced from Snowflake views.
This is to ensure that no data is missing before we perform feature engineering and model training.
In the future, Soda Core and Great Expectations could be leveraged for further data validation.

Metering data comprises usage across one or more accounts within the Snowflake Organization. Models will be trained
on each warehouse at the organization level. The feature engineering task is part of the data preparation DAG.

#### Model Training

Multiple models and model instances are trained and stored in the model catalog with versioning. Downstream inference
tasks will use the `latest` tag.

Isolation Forest model training is triggered using data-aware scheduling and will start as soon as data preparation
datasets are available.
The DAG uses dynamic tasks to train models for each warehouse.

For simplicity, we retrain the Model for each warehouse each time.

#### Predictions and Alerting

Predictions are made in batches with dynamic tasks for each model instance. Identified anomalies are grouped and a report
is generated in Markdown format. Alerts are sent as Slack messages for notification.

### Data

This project uses pre-computed data which is available in the `SNOWFLAKE` scheme for all accounts.
The `SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY` [view](https://docs.snowflake.com/en/sql-reference/organization-usage/warehouse_metering_history)
is updated nightly with metering data for all warehouses in the main account of the organization.

### Experiment Tracking

<p>
  <img src="docs/images/model_details.png" width="600" align="right"/>
</p>

This project uses [Weights and Biases](https://wandb.ai/snowpatrol/snowpatrol) for experiment and model tracking.
The DAG run_id of the training DAG to group all model instances together. Each model has an `anomaly_threshold`
parameter which is `threshold_cutoff` (default is 3) standard deviations from the mean of the stationary (decomposed
residual) scored data. Additionally, artifacts are captured to visualize the seasonal decomposition and the anomaly
scores of the training data. These are logged to the Weights and Biases project along with the model and
anomaly_threshold metric.

The link to the WANDB "run" is listed in the task logs. Future work will include integrations with a new provider which
will cross-link WandB runs with DAG runs.

<br clear="right"/>

### Model Registry

<p>
  <img src="docs/images/model_registry.png" width="400" align="right"/>
</p>

Successful runs of the training DAG tag models as "latest" in the [Model Registry](https://wandb.ai/registry/model).
Downstream DAGs use the "latest" tag for scoring, evaluation, and monitoring.

<br clear="right"/>
