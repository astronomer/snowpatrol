CREATE OR REPLACE TABLE {{ params.metrics_metering_table }} AS
/* Create a virtual table of all warehouses */
WITH virtual_warehouses AS (
    SELECT DISTINCT WAREHOUSE_NAME
    FROM {{ params.raw_metering_table }}
    WHERE WAREHOUSE_NAME IS NOT NULL
    ORDER BY WAREHOUSE_NAME
),
/* Create a cross product of all warehouses and all dates */
cross_product AS (
    SELECT  WAREHOUSE_NAME,
            USAGE_DATE
    FROM virtual_warehouses
    CROSS JOIN (
        SELECT USAGE_DATE
        FROM {{ params.common_calendar_table }}
        WHERE USAGE_DATE <= '{{ ds }}'
    ) AS calendar
),
/* Join the metering data to the cross product to have a complete matrix */
metering AS (
    SELECT cross_product.WAREHOUSE_NAME,
           cross_product.USAGE_DATE,
           COALESCE(SUM(metering.CREDITS_USED), 0) AS CREDITS_USED,
           COALESCE(SUM(metering.CREDITS_USED_COMPUTE), 0) AS CREDITS_USED_COMPUTE,
           COALESCE(SUM(metering.CREDITS_USED_CLOUD_SERVICES), 0) AS CREDITS_USED_CLOUD_SERVICES
    FROM cross_product
    LEFT JOIN {{ params.raw_metering_table }} AS metering
    ON  cross_product.USAGE_DATE        = metering.USAGE_DATE
    AND cross_product.WAREHOUSE_NAME    = metering.WAREHOUSE_NAME
    GROUP BY
        cross_product.WAREHOUSE_NAME,
        cross_product.USAGE_DATE
)
/* Compute Simple Moving averages and Standard Deviations */
SELECT  WAREHOUSE_NAME,
        USAGE_DATE,
        CREDITS_USED,
        CREDITS_USED_COMPUTE,
        CREDITS_USED_CLOUD_SERVICES,
        AVG(CREDITS_USED) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY USAGE_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS CREDITS_USED_SMA30,
        AVG(CREDITS_USED_COMPUTE) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY USAGE_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS CREDITS_USED_COMPUTE_SMA30,
        AVG(CREDITS_USED_CLOUD_SERVICES) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY USAGE_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS CREDITS_USED_CLOUD_SERVICES_SMA30,
        STDDEV(CREDITS_USED) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY USAGE_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS CREDITS_USED_STD30,
        STDDEV(CREDITS_USED_COMPUTE) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY USAGE_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS CREDITS_USED_COMPUTE_STD30,
        STDDEV(CREDITS_USED_CLOUD_SERVICES) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY USAGE_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS CREDITS_USED_CLOUD_SERVICES_STD30
FROM metering
ORDER BY WAREHOUSE_NAME,
         USAGE_DATE;
