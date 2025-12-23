

/*
    Fact table for Yellow Taxi trips
    Cleaned and validated trip data with computed metrics
*/

WITH source AS (
    SELECT * FROM `data-curriculum-478510`.`nyc_taxi_silver`.`stg_yellow_taxi`

    
    WHERE pickup_datetime >= (SELECT MAX(pickup_datetime) FROM `data-curriculum-478510`.`nyc_taxi_silver`.`fct_trips`)
    
),

cleaned AS (
    SELECT
        *,
        -- Generate a unique trip ID
        FARM_FINGERPRINT(
            CONCAT(
                CAST(pickup_datetime AS STRING),
                CAST(dropoff_datetime AS STRING),
                CAST(pickup_location_id AS STRING),
                CAST(dropoff_location_id AS STRING),
                CAST(fare_amount AS STRING)
            )
        ) AS trip_id,

        -- Extract date/time components
        DATE(pickup_datetime) AS pickup_date,
        DATE(dropoff_datetime) AS dropoff_date,
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
        EXTRACT(HOUR FROM dropoff_datetime) AS dropoff_hour,
        EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_day_of_week,

        -- Calculate trip duration in minutes
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_minutes,

        -- Calculate speed (mph)
        SAFE_DIVIDE(
            trip_distance_miles,
            TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) / 60.0
        ) AS avg_speed_mph

    FROM source
),

validated AS (
    SELECT
        trip_id,
        vendor_id,

        -- Timestamps
        pickup_datetime,
        dropoff_datetime,
        pickup_date,
        dropoff_date,
        pickup_hour,
        dropoff_hour,
        pickup_day_of_week,

        -- Locations
        pickup_location_id,
        dropoff_location_id,

        -- Trip metrics
        passenger_count,
        trip_distance_miles,
        trip_duration_minutes,
        avg_speed_mph,

        -- Codes
        rate_code_id,
        store_and_fwd_flag,
        payment_type_id,

        -- Fare breakdown
        fare_amount,
        extra_charges,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,

        -- Computed metrics
        SAFE_DIVIDE(tip_amount, fare_amount) * 100 AS tip_percentage,

        -- Data quality flags
        CASE
            WHEN trip_distance_miles <= 0 THEN TRUE
            WHEN trip_duration_minutes <= 0 THEN TRUE
            WHEN fare_amount < 0 THEN TRUE
            WHEN total_amount < 0 THEN TRUE
            WHEN passenger_count <= 0 OR passenger_count > 9 THEN TRUE
            WHEN avg_speed_mph > 100 THEN TRUE  -- Unrealistic speed
            WHEN pickup_datetime > dropoff_datetime THEN TRUE
            ELSE FALSE
        END AS has_data_quality_issue,

        _loaded_at

    FROM cleaned
    WHERE
        -- Basic validity filters
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND pickup_datetime < dropoff_datetime
        AND trip_distance_miles >= 0
        AND fare_amount >= 0
)

SELECT * FROM validated