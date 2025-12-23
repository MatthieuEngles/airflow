-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `data-curriculum-478510`.`nyc_taxi_silver`.`fct_trips` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.trip_id = DBT_INTERNAL_DEST.trip_id))

    
    when matched then update set
        `trip_id` = DBT_INTERNAL_SOURCE.`trip_id`,`vendor_id` = DBT_INTERNAL_SOURCE.`vendor_id`,`pickup_datetime` = DBT_INTERNAL_SOURCE.`pickup_datetime`,`dropoff_datetime` = DBT_INTERNAL_SOURCE.`dropoff_datetime`,`pickup_date` = DBT_INTERNAL_SOURCE.`pickup_date`,`dropoff_date` = DBT_INTERNAL_SOURCE.`dropoff_date`,`pickup_hour` = DBT_INTERNAL_SOURCE.`pickup_hour`,`dropoff_hour` = DBT_INTERNAL_SOURCE.`dropoff_hour`,`pickup_day_of_week` = DBT_INTERNAL_SOURCE.`pickup_day_of_week`,`pickup_location_id` = DBT_INTERNAL_SOURCE.`pickup_location_id`,`dropoff_location_id` = DBT_INTERNAL_SOURCE.`dropoff_location_id`,`passenger_count` = DBT_INTERNAL_SOURCE.`passenger_count`,`trip_distance_miles` = DBT_INTERNAL_SOURCE.`trip_distance_miles`,`trip_duration_minutes` = DBT_INTERNAL_SOURCE.`trip_duration_minutes`,`avg_speed_mph` = DBT_INTERNAL_SOURCE.`avg_speed_mph`,`rate_code_id` = DBT_INTERNAL_SOURCE.`rate_code_id`,`store_and_fwd_flag` = DBT_INTERNAL_SOURCE.`store_and_fwd_flag`,`payment_type_id` = DBT_INTERNAL_SOURCE.`payment_type_id`,`fare_amount` = DBT_INTERNAL_SOURCE.`fare_amount`,`extra_charges` = DBT_INTERNAL_SOURCE.`extra_charges`,`mta_tax` = DBT_INTERNAL_SOURCE.`mta_tax`,`tip_amount` = DBT_INTERNAL_SOURCE.`tip_amount`,`tolls_amount` = DBT_INTERNAL_SOURCE.`tolls_amount`,`improvement_surcharge` = DBT_INTERNAL_SOURCE.`improvement_surcharge`,`congestion_surcharge` = DBT_INTERNAL_SOURCE.`congestion_surcharge`,`airport_fee` = DBT_INTERNAL_SOURCE.`airport_fee`,`total_amount` = DBT_INTERNAL_SOURCE.`total_amount`,`tip_percentage` = DBT_INTERNAL_SOURCE.`tip_percentage`,`has_data_quality_issue` = DBT_INTERNAL_SOURCE.`has_data_quality_issue`,`_loaded_at` = DBT_INTERNAL_SOURCE.`_loaded_at`
    

    when not matched then insert
        (`trip_id`, `vendor_id`, `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `dropoff_date`, `pickup_hour`, `dropoff_hour`, `pickup_day_of_week`, `pickup_location_id`, `dropoff_location_id`, `passenger_count`, `trip_distance_miles`, `trip_duration_minutes`, `avg_speed_mph`, `rate_code_id`, `store_and_fwd_flag`, `payment_type_id`, `fare_amount`, `extra_charges`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `total_amount`, `tip_percentage`, `has_data_quality_issue`, `_loaded_at`)
    values
        (`trip_id`, `vendor_id`, `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `dropoff_date`, `pickup_hour`, `dropoff_hour`, `pickup_day_of_week`, `pickup_location_id`, `dropoff_location_id`, `passenger_count`, `trip_distance_miles`, `trip_duration_minutes`, `avg_speed_mph`, `rate_code_id`, `store_and_fwd_flag`, `payment_type_id`, `fare_amount`, `extra_charges`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `total_amount`, `tip_percentage`, `has_data_quality_issue`, `_loaded_at`)


    