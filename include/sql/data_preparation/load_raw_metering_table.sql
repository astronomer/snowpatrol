MERGE INTO {{ params.raw_metering_table }} AS target
USING (
    SELECT ACCOUNT_NAME,
           SERVICE_TYPE,
           TO_DATE(END_TIME) AS USAGE_DATE,
           WAREHOUSE_NAME,
           SUM(CREDITS_USED) AS CREDITS_USED,
           SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE,
           SUM(CREDITS_USED_CLOUD_SERVICES) AS CREDITS_USED_CLOUD_SERVICES,
           SYSDATE() AS UPDATED_AT
    FROM {{ params.source_metering_table }}
    WHERE   WAREHOUSE_ID > 0  -- Skip pseudo-VWs such as "CLOUD_SERVICES_ONLY"
    AND     USAGE_DATE BETWEEN DATEADD(DAY, -2, '{{ ds }}') AND '{{ ds }}'
    AND     ACCOUNT_NAME = '{{ params.account_number }}'
    GROUP BY ACCOUNT_NAME,
             SERVICE_TYPE,
             USAGE_DATE,
             WAREHOUSE_NAME
) AS source
ON  source.ACCOUNT_NAME     = target.ACCOUNT_NAME
AND source.SERVICE_TYPE     = target.SERVICE_TYPE
AND source.USAGE_DATE       = target.USAGE_DATE
AND source.WAREHOUSE_NAME   = target.WAREHOUSE_NAME
WHEN MATCHED THEN
    UPDATE SET
        target.ACCOUNT_NAME                 = source.ACCOUNT_NAME,
        target.SERVICE_TYPE                 = source.SERVICE_TYPE,
        target.USAGE_DATE                   = source.USAGE_DATE,
        target.WAREHOUSE_NAME               = source.WAREHOUSE_NAME,
        target.CREDITS_USED                 = source.CREDITS_USED,
        target.CREDITS_USED_COMPUTE         = source.CREDITS_USED_COMPUTE,
        target.CREDITS_USED_CLOUD_SERVICES  = source.CREDITS_USED_CLOUD_SERVICES,
        target.UPDATED_AT                   = source.UPDATED_AT
WHEN NOT MATCHED THEN
    INSERT (
        ACCOUNT_NAME,
        SERVICE_TYPE,
        USAGE_DATE,
        WAREHOUSE_NAME,
        CREDITS_USED,
        CREDITS_USED_COMPUTE,
        CREDITS_USED_CLOUD_SERVICES,
        UPDATED_AT
    ) VALUES (
        source.ACCOUNT_NAME,
        source.SERVICE_TYPE,
        source.USAGE_DATE,
        source.WAREHOUSE_NAME,
        source.CREDITS_USED,
        source.CREDITS_USED_COMPUTE,
        source.CREDITS_USED_CLOUD_SERVICES,
        source.UPDATED_AT
    );
