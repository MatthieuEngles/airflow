{{
    config(
        materialized='table'
    )
}}

/*
    Time dimension table
    All hours of the day with useful attributes
*/

WITH hours AS (
    SELECT hour FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour
)

SELECT
    hour AS hour_key,
    hour AS hour_of_day,

    -- Time periods
    CASE
        WHEN hour BETWEEN 0 AND 5 THEN 'Night'
        WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_period,

    -- Rush hour flags
    CASE
        WHEN hour BETWEEN 7 AND 9 THEN TRUE
        WHEN hour BETWEEN 17 AND 19 THEN TRUE
        ELSE FALSE
    END AS is_rush_hour,

    -- Peak hours (broader definition)
    CASE
        WHEN hour BETWEEN 6 AND 10 THEN 'Morning Peak'
        WHEN hour BETWEEN 16 AND 20 THEN 'Evening Peak'
        WHEN hour BETWEEN 11 AND 15 THEN 'Midday'
        ELSE 'Off-Peak'
    END AS peak_category,

    -- Display format
    FORMAT('%02d:00', hour) AS hour_display,
    FORMAT('%02d:00 - %02d:59', hour, hour) AS hour_range

FROM hours
