from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Snowflake Configuration
snowflake_conn_id = "snowflake_admin"
snowflake_hook = SnowflakeHook(snowflake_conn_id)
bind_name = "snowsurge"
