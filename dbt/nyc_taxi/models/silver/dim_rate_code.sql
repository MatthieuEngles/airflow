{{
    config(
        materialized='table'
    )
}}

/*
    Dimension table for rate codes
*/

SELECT
    rate_code_id,
    CASE rate_code_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_code_name
FROM (
    SELECT DISTINCT rate_code_id
    FROM {{ ref('stg_yellow_taxi') }}
    WHERE rate_code_id IS NOT NULL
)
