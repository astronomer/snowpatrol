CREATE OR REPLACE VIEW {{ params.view_name }} AS (

    WITH DAG_COST_DELTA_W_TRIGGER AS (
        SELECT
            *,
            CASE
                WHEN
                    ABS(DOLLAR_DELTA_YESTERDAY) > 25
                    AND ABS(PCT_CHANGE_YESTERDAY) > 0.33 THEN 'yesterday'
                WHEN
                    ABS(DOLLAR_DELTA_LAST_WEEK) > 25
                    AND ABS(PCT_CHANGE_LAST_WEEK) > 0.33 THEN 'last week'
                WHEN
                    ABS(DOLLAR_DELTA_LAST_MONTH) > 25
                    AND ABS(PCT_CHANGE_LAST_MONTH) > 0.33 THEN 'last month'
            END AS TRIGGER_PERIOD,
            CASE
                WHEN
                    ABS(YESTERDAY_WARN_DOLLAR_DELTA_YESTERDAY) > 25
                    AND ABS(YESTERDAY_WARN_PCT_CHANGE_YESTERDAY) > 0.33
                    THEN 'yesterday'
                WHEN
                    ABS(YESTERDAY_WARN_DOLLAR_DELTA_LAST_WEEK) > 25
                    AND ABS(YESTERDAY_WARN_PCT_CHANGE_LAST_WEEK) > 0.33
                    THEN 'last week'
                WHEN
                    ABS(YESTERDAY_WARN_DOLLAR_DELTA_LAST_MONTH) > 25
                    AND ABS(YESTERDAY_WARN_PCT_CHANGE_LAST_MONTH) > 0.33
                    THEN 'last month'
            END AS YESTERDAY_WARN_TRIGGER_PERIOD
        FROM
            {{ params.reporting_dag_cost_delta }}
        WHERE
            (
                ABS(DOLLAR_DELTA_YESTERDAY) > 25
                AND ABS(PCT_CHANGE_YESTERDAY) > 0.33
            )
            OR
            (
                ABS(DOLLAR_DELTA_LAST_WEEK) > 25
                AND ABS(PCT_CHANGE_LAST_WEEK) > 0.33
            )
            OR
            (
                ABS(DOLLAR_DELTA_LAST_MONTH) > 25
                AND ABS(PCT_CHANGE_LAST_MONTH) > 0.33
            )

    ),

    DAG_COST_DELTA_WARNINGS AS (
        SELECT
            *,
            CASE
                WHEN
                    TRIGGER_PERIOD = 'yesterday'
                    AND PCT_CHANGE_YESTERDAY > 0 THEN 'increase'
                WHEN
                    TRIGGER_PERIOD = 'last week'
                    AND PCT_CHANGE_LAST_WEEK > 0 THEN 'increase'
                WHEN
                    TRIGGER_PERIOD = 'last month'
                    AND PCT_CHANGE_LAST_MONTH > 0 THEN 'increase'
                WHEN
                    TRIGGER_PERIOD = 'yesterday'
                    AND PCT_CHANGE_YESTERDAY < 0 THEN 'decrease'
                WHEN
                    TRIGGER_PERIOD = 'last week'
                    AND PCT_CHANGE_LAST_WEEK < 0 THEN 'decrease'
                WHEN
                    TRIGGER_PERIOD = 'last month'
                    AND PCT_CHANGE_LAST_MONTH < 0 THEN 'decrease'
            END AS CHANGE_TYPE,
            CASE
                WHEN
                    YESTERDAY_WARN_TRIGGER_PERIOD = 'yesterday'
                    AND YESTERDAY_WARN_PCT_CHANGE_YESTERDAY > 0 THEN 'increase'
                WHEN
                    YESTERDAY_WARN_TRIGGER_PERIOD = 'last week'
                    AND YESTERDAY_WARN_PCT_CHANGE_LAST_WEEK > 0 THEN 'increase'
                WHEN
                    YESTERDAY_WARN_TRIGGER_PERIOD = 'last month'
                    AND YESTERDAY_WARN_PCT_CHANGE_LAST_MONTH > 0 THEN 'increase'
                WHEN
                    YESTERDAY_WARN_TRIGGER_PERIOD = 'yesterday'
                    AND YESTERDAY_WARN_PCT_CHANGE_YESTERDAY < 0 THEN 'decrease'
                WHEN
                    YESTERDAY_WARN_TRIGGER_PERIOD = 'last week'
                    AND YESTERDAY_WARN_PCT_CHANGE_LAST_WEEK < 0 THEN 'decrease'
                WHEN
                    YESTERDAY_WARN_TRIGGER_PERIOD = 'last month'
                    AND YESTERDAY_WARN_PCT_CHANGE_LAST_MONTH < 0 THEN 'decrease'
            END AS YESTERDAY_WARN_CHANGE_TYPE
        FROM DAG_COST_DELTA_W_TRIGGER

    --checking if there was a warning yesterday for the same period as today and if there
    --was both decrease and an increase for 2 days in a row, we ignore that row (cost going back to normal state)
    ),

    DAG_COST_DELTA_WARNINGS_FILTER AS (
        SELECT
            *,
            NOT COALESCE(
                (TRIGGER_PERIOD = YESTERDAY_WARN_TRIGGER_PERIOD)
                AND (CHANGE_TYPE <> YESTERDAY_WARN_CHANGE_TYPE),
                FALSE
            ) AS NO_CONSECUTIVE_WARNING
        FROM
            DAG_COST_DELTA_WARNINGS
    )

    SELECT
        DS,
        AIRFLOW_DAG_ID,
        AIRFLOW_TASK_ID,
        WAREHOUSE_NAME,
        TASK_DOLLAR_COST_TODAY,
        TASK_CREDIT_COST_TODAY,
        TASK_DOLLAR_COST_YESTERDAY,
        TASK_CREDIT_COST_YESTERDAY,
        TASK_DOLLAR_COST_LAST_WEEK,
        TASK_CREDIT_COST_LAST_WEEK,
        TASK_DOLLAR_COST_LAST_MONTH,
        TASK_CREDIT_COST_LAST_MONTH,
        DOLLAR_DELTA_YESTERDAY,
        CREDIT_DELTA_YESTERDAY,
        DOLLAR_DELTA_LAST_WEEK,
        CREDIT_DELTA_LAST_WEEK,
        DOLLAR_DELTA_LAST_MONTH,
        CREDIT_DELTA_LAST_MONTH,
        PCT_CHANGE_YESTERDAY,
        PCT_CHANGE_LAST_WEEK,
        PCT_CHANGE_LAST_MONTH,
        TRIGGER_PERIOD,
        CHANGE_TYPE,
        YESTERDAY_WARN_TRIGGER_PERIOD,
        YESTERDAY_WARN_CHANGE_TYPE
    FROM DAG_COST_DELTA_WARNINGS_FILTER
    WHERE NO_CONSECUTIVE_WARNING = TRUE
);
