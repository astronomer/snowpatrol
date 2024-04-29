# Description: This file contains the shared configurations used in all the Dags.

import os

account_number = os.getenv("SNOWFLAKE_ACCOUNT_NUMBER")
db = os.getenv("SNOWFLAKE_DATASET_DB")
schema = os.getenv("SNOWFLAKE_DATASET_SCHEMA")
# Dollars per Credit https://www.snowflake.com/en/data-cloud/pricing-options/
snowflake_credit_cost = 2.00
