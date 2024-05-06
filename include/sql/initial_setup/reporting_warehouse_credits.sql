CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    WAREHOUSE_NAME VARCHAR(),
    WAREHOUSE_CREDITS NUMBER(38, 9),
    TOTAL_TIME_ELAPSED NUMBER(38, 0),
    EFFECTIVE_RATE NUMBER(38, 9),
    DS DATE
);
