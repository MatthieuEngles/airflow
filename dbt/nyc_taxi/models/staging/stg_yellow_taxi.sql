{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Yellow Taxi data
    Reads from external table pointing to GCS parquet files
    Standardizes column names and basic type casting
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'yellow_taxi_external') }}
),

renamed AS (
    SELECT
        -- Trip identifiers
        CAST(VendorID AS INT64) AS vendor_id,

        -- Timestamps
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        -- Trip details
        CAST(passenger_count AS INT64) AS passenger_count,
        CAST(trip_distance AS FLOAT64) AS trip_distance_miles,

        -- Location IDs (post-2016 format)
        CAST(PULocationID AS INT64) AS pickup_location_id,
        CAST(DOLocationID AS INT64) AS dropoff_location_id,

        -- Rate and payment
        CAST(RatecodeID AS INT64) AS rate_code_id,
        CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
        CAST(payment_type AS INT64) AS payment_type_id,

        -- Fare components
        CAST(fare_amount AS FLOAT64) AS fare_amount,
        CAST(extra AS FLOAT64) AS extra_charges,
        CAST(mta_tax AS FLOAT64) AS mta_tax,
        CAST(tip_amount AS FLOAT64) AS tip_amount,
        CAST(tolls_amount AS FLOAT64) AS tolls_amount,
        CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
        CAST(total_amount AS FLOAT64) AS total_amount,
        CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
        CAST(Airport_fee AS FLOAT64) AS airport_fee,

        -- Metadata
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM source
)

SELECT * FROM renamed
