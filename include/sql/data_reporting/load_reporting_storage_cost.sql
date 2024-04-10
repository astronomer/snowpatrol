CREATE OR REPLACE TABLE {{ params.reporting_storage_cost }} AS
SELECT
    usage_date,
    database_id,
    database_name,
    average_database_bytes AS database_bytes,
    (database_bytes / POWER(1024, 4)) * 23.00 AS database_storage_cost,
    SYSDATE() AS last_updated_at
FROM snowflake.account_usage.database_storage_usage_history
