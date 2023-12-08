<p align="center">
  <img src="include/images/logo.png" width="300" align="right"/>
</p>

# Snowstorm
Snowstorm is an application for anomaly detection and alerting of Snowflake usage powered by Machine Learning.  Additionally, this application provides a reference implementation and template for Machine Learning Operations (MLOps) with Apache Airflow.

## MLOps Themes
In addition to implementing a specific use-case. This reference implementation provides a framework for general implementation of MLOps.  As such there are a couple of themes present in the framework which can be extended (or not used if approrpriate) for use-case specific needs.  
### Operationalize models while allowing Data Scientist choice.
For MLOps the focus is on deployment, scoring, evaluation, monitoring, alerting and periodic retraining of models.  As such we assume that Data Scientists have used their tool of choice for data exploration, model development and experimentation.  The starting point, therefore, may be a Jupyter notebook or other code provided by a Data Scientist or checked into a code repository.
###  Use [Data aware scheduling](https://docs.astronomer.io/learn/airflow-datasets) to separate workflows based on various scheduling requirements.  
It will often be necessary to schedule data engineering, feature engineering and, model training, evaluation, prediction and monitoring at different intervals. Data engineering tasks may be run hourly (often by a separate team) with nightly feature enginering and weekly inference.  Likewise model training may be triggered manually when monitoring workflows identify a need for retraining. Creating separate Airflow DAGs for each of these workflows allows them to be scheduled individually as idempotent units of work.  These workflows are, however, inter-related and Airflow's Data-aware Scheduling with Datasets allows DAG execution to occur in in relation to other DAGs.
### Use [Dynamic Tasks](https://docs.astronomer.io/learn/dynamic-tasks) to parallelize workflows.  
Airflow is well suited for efficiently running tasks in parallel.  Hyperparameter optimization, A/B testing, training champion/challenger models or training many unit-specific models (ie. forecastng models for many individual components) all benefit from parallelization.  Airflow's Dynamic Task mapping makes it possible to easily iterate over a list of ML operations.
### Score in batch.
While some use-cases require low-latency inference and online scoring many will be most efficient with batch scoring. Airflow supports batch scoring at various intervals. Model predictions can be made in Airflow tasks by importing a model or by REST calls to a deployed endpoint.
### Leverage centralized auditing and lineage.
By combining MLOps with the rest of an organziations Data Ops Airflow provides a single view to assist in reproducibility and explainability. With the ability to track backwards from model predictions to model training and the datasets that were used ML Engineers and Data Scientists can more easily identify issues or perform post-moretm analysis.
### Integrate with model registries and ML tools of choice
ML models are "living software" requiring the ability to track state over time.  Many teams will leverage model registries like [Weights and Biases](https://wandb.ai) or [MLFlow](https://mlflow.org).  Airflow integrates well with these services and allows logging models after training and downloading models during scoring.  Many Airflow providers are available for tools like [SageMaker](), [VertexAI](), [AzureML]() and [Databricks]().  Additionally, many NLP use-cases will rely on a vector database and Airflow includes providers for services like [Weaviate](), [Pinecone](), [pgvector](), [OpenSearch]() as well as LLM providers like [OpenAI]() and [Cohere]().

todo
    multiple models (c/c, a/b)
    backfill
    worker queues

  
## Project Structure
### Business Objective
Cloud-based services like Snowflake provide many benefits to organizations by simplifying operations, reducing cost and delays and enabling self-service for many teams. However, this added agility and self-service empowerment can result in unexpected costs despite good governance processes.
  
This project aims to identify anomalous usage activity in order to allow management and timely correction of activities.

### Modeling

Snowflake users incur cost for compute, storage, marketplace and various other services. Of these compute is the most significant but others cannot be ignored. Data exploration and experimentation was performed (see [notebook](include/snowstorm.ipynb)) to evaluate the ability to idnetify anomalies with compute, storage and a sum of all costs ("total_usage").  While storage usage can increase quickly the relative cost is not significant and will also be captured in the total_usage. Production models, therefore, will monitor compute usage specifically as well as combined usage.

<p align="center">
  <img src="include/images/decompose.png" width="400" align="left"/>
</p>  
  
Without specific labeled data we will use an unsupervised approach with an [Isolation Forest]() model.  Separate models will be trained for total_usage and compute using dynamic tasks.  Because the usage demonstrates both seasonality and increasing trend the model will be trained on decomposed residuals factoring out both seasonality and trend.
<br clear="left"/>

  
### DAGs
<p align="center">
  <img src="include/images/datasets.png" width="400" align="right"/>
</p>
This project sparates data engineering, feature engineering, training, prediction and monitoring into separate DAGs with Airflow Dataset scheduling. 
  
Note: For this use-case the DAGs are very small with only a couple of tasks each.  As a reference implementation the use-case was selected because of its simplicity and to highlight the framework without distractions of the specific use-case.
  
#### Data Engineering
Snowflake runs nightly jobs to capture usage statistics for billing (see Dataset section below.).  However the usage data is only available to administrators by default.  Additionally there are different levels of aggregation which happen at the "organization" and "account" levels. The data engineering DAG is somewhat of a placeholder as its primary function is to extract organization-level usage to the project's schema and database for access by non-admin users.  Downstream feature engineering will pivot on the `USAGE_TYPE` column which, by default, contains whitespace.  To simplify the pivot the whitespaced features are converted to snake-cased.  The Data engineering DAG runs nightly.
#### Feature Engineering
Usage data is comprised of usage across one or more accounts within the Snowflake Organization. Models will be trained on both total_usage and compute usage at the organization level.  Features are extracted by pivoting on the USAGE_TYPE column with summation across accounts.  The feature engineering DAG is triggered from the data engineering DAG.
#### Model Training
Multiple models and model instances are trained and stored in the model catalog with versioning.  Downstream inference tasks will use the `latest` tag.
- Champion Model:  The baseline Isolation Forest model training is triggered manually for the initial model and  uses dynamic tasks to train models for cost categories (initially "total_usage" and "compute").  Other cost models can be trained by updating the `cost_models` extra of the `isolation_forest_model` Dataset.
- Challenger Model: TODO
#### Predictions and Alerting
Predictions are made in batch with dynamic tasks for each model instance.  Idenfied anomalies are grouped and a report is generated in markdown format. Alerts are sent as Slack messages for notification.
<p>
  <img src="include/images/alert.png" width="500"/>
</p>
<br clear="center"/>

#### Model Evaluation
todo
#### Model and Data Monitoring
todo

### Data
This project uses pre-computed data which is available in the `SNOWFLAKE` scheme for all accounts. 
The `SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY` [view](https://docs.snowflake.com/en/sql-reference/organization-usage/usage_in_currency_daily) is updated nightly with usage data for all usage types in all accounts in the organization.
  
### Experiment Tracking
<p>
  <img src="include/images/model_details.png" width="600" align="right"/>
</p>
  
This project uses [Weights and Biases](https://wandb.ai/snowstorm/snowstorm) for experiment and model tracking. 
The DAG run_id of the training DAG to group all model instances together.  Each model has an `anomaly_threshold` parameter which is `threshold_cutoff` (default is 3) standard deviations from the mean of the stationary (decomposed residual) scored data.  Additionally artifacts are captured to visualize the seasonal decomposition and the anomaly scores of the training data.  These are logged to the Weights and Biases project along with the model and anomaly_threshold metric.

The link to the WANDB "run" is listed in the task logs.  Future work will include integrations with a new provider which will cross-link WandB runs with DAG runs.
TODO: add wandb link in operator extra links
<br clear="right"/>

### Model Registry
<p>
  <img src="include/images/model_registry.png" width="400" align="right"/>
</p>
  
Successful runs of the training DAG tag models as "latest" in the [Model Registry](https://wandb.ai/registry/model).  Downstream DAGs use the "latest" tag for scoring, evaluation and monitoring.  Future work will include CI/CD integrations which notify administrators of a new model for manual promotion to "latest".

TODO: tag models for manual review.

<br clear="right"/>

## Project Setup

1. 