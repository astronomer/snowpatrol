CREATE OR REPLACE VIEW {{ params.view_name }} AS (
    SELECT
        T.WAREHOUSE_NAME,
        T.WAREHOUSE_CREDITS,
        T.DS,
        T.WAREHOUSE_CREDITS - Y.WAREHOUSE_CREDITS AS WAREHOUSE_CREDITS_DELTA_YESTERDAY
    FROM {{ params.reporting_warehouse_credits }} AS T
    LEFT JOIN {{ params.reporting_warehouse_credits }} AS Y
        ON
            T.WAREHOUSE_NAME = Y.WAREHOUSE_NAME
            AND T.DS = DATEADD(DAY, 1, Y.DS)
);
