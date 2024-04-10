MERGE INTO {{ params.reporting_query_duration }} AS target
USING (
    WITH warehouse_sizes AS (
        SELECT 'X-Small'  AS warehouse_size, 1   AS credits_per_hour UNION ALL
        SELECT 'Small'    AS warehouse_size, 2   AS credits_per_hour UNION ALL
        SELECT 'Medium'   AS warehouse_size, 4   AS credits_per_hour UNION ALL
        SELECT 'Large'    AS warehouse_size, 8   AS credits_per_hour UNION ALL
        SELECT 'X-Large'  AS warehouse_size, 16  AS credits_per_hour UNION ALL
        SELECT '2X-Large' AS warehouse_size, 32  AS credits_per_hour UNION ALL
        SELECT '3X-Large' AS warehouse_size, 64  AS credits_per_hour UNION ALL
        SELECT '4X-Large' AS warehouse_size, 128 AS credits_per_hour
    )
    SELECT
        qh.query_id,
        qh.query_text,
        qh.database_name,
        qh.schema_name,
        qh.warehouse_name,
        qh.warehouse_size,
        qh.warehouse_type,
        qh.user_name,
        qh.role_name,
        DATE(qh.start_time) as start_date,
        qh.error_code,
        qh.execution_status,
        qh.execution_time/(1000) AS execution_time_sec,
        qh.total_elapsed_time/(1000) AS total_elapsed_time_sec,
        qh.rows_deleted,
        qh.rows_inserted,
        qh.rows_produced,
        qh.rows_unloaded,
        qh.rows_updated,
        TRY_PARSE_JSON(qh.query_tag):dag_id::varchar as airflow_dag_id,
        TRY_PARSE_JSON(qh.query_tag):task_id::varchar as airflow_task_id,
        TRY_PARSE_JSON(qh.query_tag):run_id::varchar as airflow_run_id,
        TRY_TO_TIMESTAMP(TRY_PARSE_JSON(qh.query_tag):logical_date::varchar) as airflow_logical_date,
        TRY_TO_TIMESTAMP(TRY_PARSE_JSON(qh.query_tag):started::varchar) as airflow_started,
        TRY_PARSE_JSON(qh.query_tag):operator::varchar as airflow_operator,
        qh.execution_time/(1000*60*60)*wh.credits_per_hour AS credits_cost,
        credits_cost * 2.00 AS dollars_cost, -- We use Snowflake Standard. See pricing: https://www.snowflake.com/en/data-cloud/pricing-options/
        CURRENT_TIMESTAMP() AS last_updated_at
    FROM snowflake.account_usage.query_history AS qh
    INNER JOIN warehouse_sizes AS wh
        ON qh.warehouse_size=wh.warehouse_size
    WHERE qh.start_time >= DATEADD(DAY, -30, CURRENT_DATE())
) AS source
ON target.query_id = source.query_id -- assuming query_id is the primary key
WHEN MATCHED THEN
    UPDATE SET
        query_text = source.query_text,
        database_name = source.database_name,
        schema_name = source.schema_name,
        warehouse_name = source.warehouse_name,
        warehouse_size = source.warehouse_size,
        warehouse_type = source.warehouse_type,
        user_name = source.user_name,
        role_name = source.role_name,
        start_date = source.start_date,
        error_code = source.error_code,
        execution_status = source.execution_status,
        execution_time_sec = source.execution_time_sec,
        total_elapsed_time_sec = source.total_elapsed_time_sec,
        rows_deleted = source.rows_deleted,
        rows_inserted = source.rows_inserted,
        rows_produced = source.rows_produced,
        rows_unloaded = source.rows_unloaded,
        rows_updated = source.rows_updated,
        airflow_dag_id = source.airflow_dag_id,
        airflow_task_id = source.airflow_task_id,
        airflow_run_id = source.airflow_run_id,
        airflow_logical_date = source.airflow_logical_date,
        airflow_started = source.airflow_started,
        airflow_operator = source.airflow_operator,
        credits_cost = source.credits_cost,
        dollars_cost = source.dollars_cost,
        last_updated_at = source.last_updated_at
WHEN NOT MATCHED THEN
    INSERT (
        query_id,
        query_text,
        database_name,
        schema_name,
        warehouse_name,
        warehouse_size,
        warehouse_type,
        user_name,
        role_name,
        start_date,
        error_code,
        execution_status,
        execution_time_sec,
        total_elapsed_time_sec,
        rows_deleted,
        rows_inserted,
        rows_produced,
        rows_unloaded,
        rows_updated,
        airflow_dag_id,
        airflow_task_id,
        airflow_run_id,
        airflow_logical_date,
        airflow_started,
        airflow_operator,
        credits_cost,
        dollars_cost,
        last_updated_at
    )
    VALUES (
        source.query_id,
        source.query_text,
        source.database_name,
        source.schema_name,
        source.warehouse_name,
        source.warehouse_size,
        source.warehouse_type,
        source.user_name,
        source.role_name,
        source.start_date,
        source.error_code,
        source.execution_status,
        source.execution_time_sec,
        source.total_elapsed_time_sec,
        source.rows_deleted,
        source.rows_inserted,
        source.rows_produced,
        source.rows_unloaded,
        source.rows_updated,
        source.airflow_dag_id,
        source.airflow_task_id,
        source.airflow_run_id,
        source.airflow_logical_date,
        source.airflow_started,
        source.airflow_operator,
        source.credits_cost,
        source.dollars_cost,
        source.last_updated_at
    );
