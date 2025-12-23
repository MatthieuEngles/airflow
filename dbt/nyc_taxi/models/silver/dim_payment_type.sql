{{
    config(
        materialized='table'
    )
}}

/*
    Dimension table for payment types
*/

SELECT
    payment_type_id,
    CASE payment_type_id
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Not specified'
    END AS payment_type_name
FROM (
    SELECT DISTINCT payment_type_id
    FROM {{ ref('stg_yellow_taxi') }}
    WHERE payment_type_id IS NOT NULL
)
