CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    USAGE_DATE DATE NOT NULL,
    YEAR SMALLINT NOT NULL,
    MONTH SMALLINT NOT NULL,
    MONTH_NAME CHAR(3) NOT NULL,
    DAY_OF_MON SMALLINT NOT NULL,
    DAY_OF_WEEK VARCHAR(9) NOT NULL,
    WEEK_OF_YEAR SMALLINT NOT NULL,
    DAY_OF_YEAR SMALLINT NOT NULL
)
AS
WITH DATES AS (
    SELECT
        '{{ dag.start_date }}'::DATE - 1
        + ROW_NUMBER() OVER (ORDER BY 0) AS USAGE_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 1826)) -- 5 years
)

SELECT
    USAGE_DATE,
    YEAR(USAGE_DATE) AS YEAR,
    MONTH(USAGE_DATE) AS MONTH,
    MONTHNAME(USAGE_DATE) AS MONTH_NAME,
    DAY(USAGE_DATE) AS DAY_OF_MON,
    DAYOFWEEK(USAGE_DATE) AS DAY_OF_WEEK,
    WEEKOFYEAR(USAGE_DATE) AS WEEK_OF_YEAR,
    DAYOFYEAR(USAGE_DATE) AS DAY_OF_YEAR
FROM DATES;
