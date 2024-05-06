DELETE FROM {{ params.reporting_query_history }}
WHERE DS = '{{ ds }}';

INSERT INTO {{ params.reporting_query_history }}
SELECT
    QUERY_ID,
    QUERY_TEXT,
    DATABASE_NAME,
    SCHEMA_NAME,
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    WAREHOUSE_TYPE,
    USER_NAME,
    ROLE_NAME,
    TO_DATE(START_TIME::TIMESTAMP_TZ) AS START_DATE,
    TO_DATE(END_TIME::TIMESTAMP_TZ) AS END_DATE,
    ERROR_CODE,
    EXECUTION_STATUS,
    EXECUTION_TIME / (1000) AS EXECUTION_TIME_SEC,
    TOTAL_ELAPSED_TIME,
    TOTAL_ELAPSED_TIME / (1000) AS TOTAL_ELAPSED_TIME_SEC,
    QUEUED_OVERLOAD_TIME,
    (QUEUED_OVERLOAD_TIME / TOTAL_ELAPSED_TIME)::FLOAT AS QUEUED_PCT,
    RANK()
        OVER (PARTITION BY WAREHOUSE_NAME ORDER BY TOTAL_ELAPSED_TIME DESC)
        AS QUERY_TIME_RANK,
    ROWS_DELETED,
    ROWS_INSERTED,
    ROWS_PRODUCED,
    ROWS_UNLOADED,
    ROWS_UPDATED,
    TRY_PARSE_JSON(QUERY_TAG):dag_id::VARCHAR AS AIRFLOW_DAG_ID,
    TRY_PARSE_JSON(QUERY_TAG):task_id::VARCHAR AS AIRFLOW_TASK_ID,
    TRY_PARSE_JSON(QUERY_TAG):run_id::VARCHAR AS AIRFLOW_RUN_ID,
    TRY_TO_TIMESTAMP(TRY_PARSE_JSON(QUERY_TAG):logical_date::VARCHAR)
        AS AIRFLOW_LOGICAL_DATE,
    TRY_TO_TIMESTAMP(TRY_PARSE_JSON(QUERY_TAG):started::VARCHAR)
        AS AIRFLOW_STARTED,
    TRY_PARSE_JSON(QUERY_TAG):operator::VARCHAR AS AIRFLOW_OPERATOR,
    '{{ ds }}' AS DS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE TO_DATE(END_TIME::TIMESTAMP_TZ) = '{{ ds }}';