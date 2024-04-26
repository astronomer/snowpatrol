<p align="center">
  <img src="docs/images/logo_rm.png" width="300" align="right"/>
</p>

# SnowPatrol

SnowPatrol is an application for anomaly detection and alerting of Snowflake usage powered by Machine Learning. It’s
also an MLOps reference implementation, an example of how to use Airflow as a way to manage the training, testing,
deployment, and monitoring of predictive models.

At Astronomer, we firmly believe in the power of open source and sharing knowledge.
We are excited to share this MLOps reference implementation with the community and hope it will be useful to others.

![anomalies.png](docs/images/anomalies_all_wh.png)

## Project Structure

### Business Objective

Astronomer is a data-driven company, and we rely heavily on Snowflake to store and analyze our data. Self-service
analytics is a core part of our culture, and we encourage our teams to answer questions with data themselves, either by
running SQL queries or building their own data visualizations. As such, a large number of users and service accounts run
queries on a daily basis; on average more than 900k queries are run daily. The majority of these queries come from
automated Airflow DAGs running on various schedules. Identifying which automated process or which user ran a query
causing increased usage on a given day is time-consuming when done manually. We would rather not waste time [chasing
cars](https://youtu.be/GemKqzILV4w).
Cost management is also a key part of our operations. Just like most organizations, we want to avoid overages and
control our Snowflake costs. While that is a common goal, it can be challenging to achieve. Snowflake costs are complex
and can be attributed to a variety of factors.

SnowPatrol aims to identify anomalous usage activity to allow management and timely correction of activities.

In addition to anomalous usage activity, SnowPatrol can also track the Snowflake costs associated with every Airflow DAG
and Task. Install the [Astronomer SnowPatrol Plugin](https://github.com/astronomer/astronomer-snowpatrol-plugin) in your
Airflow Deployments to automatically add Airflow Metadata to every Snowflake Query through Query Tags.

To understand how the ML Model was built, refer to the [MODELING](docs%2FMODELING.md) documentation page.

## Project Setup

### Prerequisites

To use SnowPatrol in your Organization you need:

- [Weights and Biases account](https://wandb.ai/signup) and an API KEY.
- [Snowflake account](https://trial.snowflake.com/?owner=SPN-PID-365384) with the necessary permissions
- [Astronomer account](https://www.astronomer.io/try-astro/)
- A [Slack app](https://api.slack.com/apps/) in the channel to be notified with an `xoxb-` oauth token with `chat:write`
  permissions.
- Docker Desktop or similar Docker services for local development.
- An external Postgres Database to use the Anomaly Exploration Plugin

#### Snowflake Permissions

Step 1: Create a Role named `snowpatrol` and grant it the USAGE_VIEWER and ORGANIZATION_BILLING_VIEWER permissions. This
is needed so that SnowPatrol can query the following Tables:

- Schemas:
    - Database SNOWFLAKE
    - Schema ORGANIZATION_USAGE
    - Schema ACCOUNT_USAGE
- Tables:
    - SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
    - SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY
    - SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
    - SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    - SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY

```sql
CREATE ROLE snowpatrol COMMENT = 'This role has USAGE_VIEWER and ORGANIZATION_BILLING_VIEWER privilege';

GRANT DATABASE ROLE USAGE_VIEWER TO ROLE snowpatrol;
GRANT DATABASE ROLE ORGANIZATION_BILLING_VIEWER TO ROLE snowpatrol;
```

Step 2: SnowPatrol also needs access to create tables and write data in a dedicated database schema.

```sql
GRANT USAGE ON DATABASE <database> TO ROLE snowpatrol;
GRANT ALL PRIVILEGES ON SCHEMA <database>.snowpatrol TO ROLE snowpatrol;
```

Step 3: Grant the Role to the User or Service Account you plan to use to connect to Snowflake.

```
GRANT ROLE snowpatrol TO <user>;
GRANT ROLE snowpatrol TO <service_account>;
```

### Setup

1. Install Astronomer's [Astro CLI](https://github.com/astronomer/astro-cli). The Astro CLI is an Apache 2.0 licensed,
   open-source tool for building Airflow instances and provides the fastest and easiest way to be up and running with
   Airflow in minutes. The Astro CLI is a Docker Compose-based tool and integrates easily with Weights and Biases for a
   local developer environment.

   To install the Astro CLI, open a terminal window and run:

   For MacOS
    ```bash
    brew install astro
    ```

   For Linux
    ```bash
    curl -sSL install.astronomer.io | sudo bash -s
    ```

2. Clone this repository:
    ```bash
    git clone https://github.com/astronomer/snowpatrol
    cd snowpatrol
    ```

3. Create a file called `.env` with the following connection strings and environment variables.
   To make this easier, we have included a .env.example file that you can rename to .env.

    - `WANDB_API_KEY`: The API KEY should have access to the Weights and Biases `snowpatrol` entity and `snowpatrol`
      project.
      Example:
      ```
      WANDB_API_KEY:'xxxxxxxxxxx'
      ```

    - `AIRFLOW_CONN_SNOWFLAKE_CONN`: This connection string is used for extracting the usage data to the project
      schema. The user should have access to a role with permissions to read
      the `SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY`
      [view](https://docs.snowflake.com/en/sql-reference/organization-usage/warehouse_metering_history)
      Example:
      ```
      AIRFLOW_CONN_SNOWFLAKE_CONN='{"conn_type": "snowflake", "login": "<username>", "password": "<password>", "schema": "<schema>", "extra": {"account": "<account>", "warehouse": "<warehouse>", "role": "<role>", "authenticator": "snowflake", "application": "AIRFLOW"}}'
      ```

    - `AIRFLOW_CONN_SLACK_API_ALERT`: Add a Slack token for sending Slack alerts.
      Example:
      ```
      AIRFLOW_CONN_SLACK_API_ALERT='{"conn_type": "slack", "password": "xoxb-<>"}'
      ```

    - `SNOWFLAKE_ACCOUNT_NUMBER`: Your Snowflake Account Number
      Example:
      ```
      SNOWFLAKE_ACCOUNT_NUMBER=<account_number>
      ```

    - `SNOWFLAKE_DATASET_DB`: The Snowflake Database to use when creating the SnowPatrol tables
      Example:
      ```
      SNOWFLAKE_DATASET_DB=<my_db>
      ```

    - `SNOWFLAKE_DATASET_SCHEMA`: The Snowflake Schema to use when creating the SnowPatrol tables
      Example:
      ```
      SNOWFLAKE_DATASET_SCHEMA=<my_schema>
      ```

4. Start Apache Airflow
    ```sh
    astro dev start
    ```

   Airflow starts a browser window that should open to [http://localhost:8080](http://localhost:8080). Log in with
   the following credentials:
    - **username**: `admin`
    - **password**: `admin`

5. Run the `initial_setup` DAG to create the necessary Snowflake tables.

6. Run the `data_preparation` DAG:
   After the `data_preparation` DAG runs it will trigger the `train_isolation_forest` DAG.

7. After the `data_preparation` and `train_isolation_forest` DAGs run, Airflow will trigger
   the `predict_isolation_forest` DAG.

8. Deploy to Astro:
   Complete the following steps to promote from Airflow running locally to a production deployment in Astro.
    - Log in to Astro from the CLI.
    ```bash
    astro login
    ```
    - [Deploy](https://docs.astronomer.io/astro/deploy-code) the Astro project.
    ```bash
    astro deployment create -n 'snowpatrol'
    astro deployment variable update -lsn 'snowpatrol'
    astro deploy -f
    ```

   The `variable update` will load variables from the `.env` file that was created in step #3.

9. Login to Astro and ensure that all the DAGs are unpaused. Every night the `data_preparation`, `data_reporting`,
   `train_isolation_forest` and `predict_isolation_forest` DAGs will run.
   Alerts will be sent to the channel specified in `slack_channel` in the `predict_isolation_forest` DAG.

10. Configure the following GitHub Environments and Secrets in GitHub to setup CI/CD.
    Secrets:
     - DEV_ASTRO_API_TOKEN:  [Documentation here](https://docs.astronomer.io/astro/deployment-api-tokens)
     - DEV_ASTRO_DEPLOYMENT_ID: Copy this value from the `ID` field in your Astro Deployment
     - PROD_ASTRO_API_TOKEN  [Documentation here](https://docs.astronomer.io/astro/deployment-api-tokens)
     - PROD_ASTRO_DEPLOYMENT_ID  Copy this value from the `ID` field in your Astro Deployment

## Feedback

Give us your feedback, comments and ideas at https://github.com/astronomer/snowpatrol/discussions
