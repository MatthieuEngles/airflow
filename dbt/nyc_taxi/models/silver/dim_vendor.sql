{{
    config(
        materialized='table'
    )
}}

/*
    Dimension table for taxi vendors
*/

SELECT
    vendor_id,
    CASE vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'VeriFone Inc.'
        ELSE 'Unknown'
    END AS vendor_name
FROM (
    SELECT DISTINCT vendor_id
    FROM {{ ref('stg_yellow_taxi') }}
    WHERE vendor_id IS NOT NULL
)
