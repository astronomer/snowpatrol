CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    WAREHOUSE_NAME VARCHAR(),
    USAGE_DATE DATE,
    CREDITS_USED NUMBER(38, 9),
    CREDITS_USED_COMPUTE NUMBER(38, 9),
    CREDITS_USED_CLOUD_SERVICES NUMBER(38, 9),
    CREDITS_USED_SMA30 NUMBER(38, 12),
    CREDITS_USED_COMPUTE_SMA30 NUMBER(38, 12),
    CREDITS_USED_CLOUD_SERVICES_SMA30 NUMBER(38, 12),
    CREDITS_USED_STD30 FLOAT,
    CREDITS_USED_COMPUTE_STD30 FLOAT,
    CREDITS_USED_CLOUD_SERVICES_STD30 FLOAT,
    DS DATE
);