CREATE OR REPLACE VIEW {{ params.view_name }} AS (
    SELECT
        M.WAREHOUSE_NAME,
        M.USAGE_DATE,
        M.CREDITS_USED,
        M.CREDITS_USED_COMPUTE,
        M.CREDITS_USED_CLOUD_SERVICES,
        M.CREDITS_USED_SMA30,
        M.CREDITS_USED_COMPUTE_SMA30,
        M.CREDITS_USED_CLOUD_SERVICES_SMA30,
        M.CREDITS_USED_STD30,
        M.CREDITS_USED_COMPUTE_STD30,
        M.CREDITS_USED_CLOUD_SERVICES_STD30,
        O.TREND,
        O.SEASONAL,
        O.RESIDUAL,
        O.SCORE,
        O.PREDICTION_DATETIME,
        IFF(O.SCORE IS NOT NULL, TRUE, FALSE) AS IS_ANOMALY
    FROM {{ params.metrics_metering_table }} AS M
    LEFT JOIN {{ params.model_output_anomalies_table }} AS O
        ON
            M.WAREHOUSE_NAME = O.WAREHOUSE_NAME
            AND M.USAGE_DATE = O.USAGE_DATE
);