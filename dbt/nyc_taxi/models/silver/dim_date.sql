{{
    config(
        materialized='table'
    )
}}

/*
    Date dimension table
    Generates all dates from 2009-01-01 to 2030-12-31
*/

WITH date_spine AS (
    SELECT
        date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY('2009-01-01', '2030-12-31', INTERVAL 1 DAY)
    ) AS date_day
)

SELECT
    date_day AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%B', date_day) AS month_name,
    FORMAT_DATE('%Y-%m', date_day) AS year_month,
    FORMAT_DATE('%Y-Q%Q', date_day) AS year_quarter,

    -- Fiscal year (assuming fiscal year starts in July)
    CASE
        WHEN EXTRACT(MONTH FROM date_day) >= 7 THEN EXTRACT(YEAR FROM date_day) + 1
        ELSE EXTRACT(YEAR FROM date_day)
    END AS fiscal_year,

    -- Boolean flags
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday  -- Can be populated with actual holidays later

FROM date_spine
