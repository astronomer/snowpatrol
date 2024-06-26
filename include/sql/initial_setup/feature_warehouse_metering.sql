CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    USAGE_DATE DATE,
    CREDITS_USED NUMBER(38, 9),
    TREND FLOAT,
    SEASONAL FLOAT,
    RESIDUAL FLOAT,
    WAREHOUSE_NAME VARCHAR(),
    DS DATE
);
