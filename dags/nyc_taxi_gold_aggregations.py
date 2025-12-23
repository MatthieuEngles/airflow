"""
NYC Yellow Cab GOLD Layer DAG

Creates business-level aggregations and metrics from SILVER data.
Stores results in BigQuery GOLD dataset for dashboard consumption.
"""

from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
import os

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'data-curriculum-478510')
BQ_DATASET_SILVER = os.environ.get('BQ_DATASET_SILVER', 'nyc_taxi_silver')
BQ_DATASET_GOLD = os.environ.get('BQ_DATASET_GOLD', 'nyc_taxi_gold')

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="nyc_taxi_gold_aggregations",
    description="Create GOLD layer business aggregations",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi", "gold", "aggregations"],
) as dag:

    @task(task_id="create_gold_dataset")
    def create_gold_dataset():
        """Ensure GOLD dataset exists."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)
        dataset_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_id} ready")
        return dataset_id

    @task(task_id="create_daily_summary")
    def create_daily_summary():
        """Create daily trip summary aggregation."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.daily_summary` AS
        SELECT
            pickup_date,
            EXTRACT(YEAR FROM pickup_date) AS year,
            EXTRACT(MONTH FROM pickup_date) AS month,
            EXTRACT(DAYOFWEEK FROM pickup_date) AS day_of_week,

            -- Volume metrics
            COUNT(*) AS total_trips,
            SUM(passenger_count) AS total_passengers,
            COUNT(DISTINCT pickup_location_id) AS unique_pickup_locations,
            COUNT(DISTINCT dropoff_location_id) AS unique_dropoff_locations,

            -- Distance metrics
            SUM(trip_distance_miles) AS total_distance_miles,
            AVG(trip_distance_miles) AS avg_distance_miles,
            APPROX_QUANTILES(trip_distance_miles, 100)[OFFSET(50)] AS median_distance_miles,

            -- Duration metrics
            SUM(trip_duration_minutes) AS total_duration_minutes,
            AVG(trip_duration_minutes) AS avg_duration_minutes,
            APPROX_QUANTILES(trip_duration_minutes, 100)[OFFSET(50)] AS median_duration_minutes,

            -- Revenue metrics
            SUM(total_amount) AS total_revenue,
            SUM(fare_amount) AS total_fare_revenue,
            SUM(tip_amount) AS total_tips,
            SUM(tolls_amount) AS total_tolls,
            AVG(total_amount) AS avg_trip_revenue,
            AVG(tip_amount) AS avg_tip,
            AVG(SAFE_DIVIDE(tip_amount, fare_amount) * 100) AS avg_tip_percentage,

            -- Payment breakdown
            COUNTIF(payment_type_id = 1) AS credit_card_trips,
            COUNTIF(payment_type_id = 2) AS cash_trips,

            -- Data quality
            COUNTIF(has_data_quality_issue) AS trips_with_issues,
            ROUND(COUNTIF(has_data_quality_issue) / COUNT(*) * 100, 2) AS pct_trips_with_issues

        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
        GROUP BY pickup_date
        ORDER BY pickup_date
        """

        client.query(query).result()
        print("Created daily_summary table")

    @task(task_id="create_hourly_patterns")
    def create_hourly_patterns():
        """Create hourly trip patterns aggregation."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.hourly_patterns` AS
        SELECT
            pickup_hour,
            pickup_day_of_week,
            CASE pickup_day_of_week
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS day_name,
            CASE
                WHEN pickup_day_of_week IN (1, 7) THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type,

            COUNT(*) AS total_trips,
            AVG(trip_distance_miles) AS avg_distance,
            AVG(trip_duration_minutes) AS avg_duration,
            AVG(total_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            AVG(avg_speed_mph) AS avg_speed

        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
        WHERE NOT has_data_quality_issue
        GROUP BY pickup_hour, pickup_day_of_week
        ORDER BY pickup_day_of_week, pickup_hour
        """

        client.query(query).result()
        print("Created hourly_patterns table")

    @task(task_id="create_monthly_trends")
    def create_monthly_trends():
        """Create monthly trends aggregation."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.monthly_trends` AS
        WITH monthly_agg AS (
            SELECT
                EXTRACT(YEAR FROM pickup_date) AS year,
                EXTRACT(MONTH FROM pickup_date) AS month,
                FORMAT_DATE('%Y-%m', pickup_date) AS year_month,
                MIN(pickup_date) AS month_start_date,

                -- Volume
                COUNT(*) AS total_trips,
                SUM(passenger_count) AS total_passengers,
                COUNT(*) / COUNT(DISTINCT pickup_date) AS avg_daily_trips,

                -- Revenue
                SUM(total_amount) AS total_revenue,
                SUM(fare_amount) AS fare_revenue,
                SUM(tip_amount) AS tip_revenue,
                SUM(total_amount) / COUNT(*) AS revenue_per_trip,

                -- Efficiency
                AVG(trip_distance_miles) AS avg_distance,
                AVG(trip_duration_minutes) AS avg_duration,
                AVG(avg_speed_mph) AS avg_speed

            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
            WHERE NOT has_data_quality_issue
            GROUP BY year, month, year_month
        )
        SELECT
            *,
            LAG(total_trips) OVER (ORDER BY year, month) AS prev_month_trips,
            LAG(total_revenue) OVER (ORDER BY year, month) AS prev_month_revenue
        FROM monthly_agg
        ORDER BY year, month
        """

        client.query(query).result()
        print("Created monthly_trends table")

    @task(task_id="create_location_stats")
    def create_location_stats():
        """Create location-based statistics."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.location_stats` AS
        WITH pickup_stats AS (
            SELECT
                pickup_location_id AS location_id,
                'pickup' AS location_type,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(total_amount) AS avg_fare
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
            WHERE NOT has_data_quality_issue
            GROUP BY pickup_location_id
        ),
        dropoff_stats AS (
            SELECT
                dropoff_location_id AS location_id,
                'dropoff' AS location_type,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(total_amount) AS avg_fare
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
            WHERE NOT has_data_quality_issue
            GROUP BY dropoff_location_id
        ),
        combined AS (
            SELECT * FROM pickup_stats
            UNION ALL
            SELECT * FROM dropoff_stats
        )
        SELECT
            location_id,
            SUM(CASE WHEN location_type = 'pickup' THEN trip_count ELSE 0 END) AS pickup_trips,
            SUM(CASE WHEN location_type = 'dropoff' THEN trip_count ELSE 0 END) AS dropoff_trips,
            SUM(trip_count) AS total_trips,
            SUM(total_revenue) AS total_revenue,
            AVG(avg_fare) AS avg_fare,
            RANK() OVER (ORDER BY SUM(trip_count) DESC) AS popularity_rank
        FROM combined
        GROUP BY location_id
        ORDER BY total_trips DESC
        """

        client.query(query).result()
        print("Created location_stats table")

    @task(task_id="create_payment_analysis")
    def create_payment_analysis():
        """Create payment type analysis."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.payment_analysis` AS
        WITH payment_agg AS (
            SELECT
                EXTRACT(YEAR FROM pickup_date) AS year,
                EXTRACT(MONTH FROM pickup_date) AS month,
                FORMAT_DATE('%Y-%m', pickup_date) AS year_month,
                payment_type_id,
                CASE payment_type_id
                    WHEN 1 THEN 'Credit Card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No Charge'
                    WHEN 4 THEN 'Dispute'
                    WHEN 5 THEN 'Unknown'
                    WHEN 6 THEN 'Voided'
                    ELSE 'Other'
                END AS payment_type_name,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(total_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip,
                AVG(SAFE_DIVIDE(tip_amount, fare_amount) * 100) AS avg_tip_pct
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
            WHERE NOT has_data_quality_issue
            GROUP BY year, month, year_month, payment_type_id
        )
        SELECT
            *,
            ROUND(trip_count * 100.0 / SUM(trip_count) OVER (PARTITION BY year, month), 2) AS pct_of_monthly_trips
        FROM payment_agg
        ORDER BY year, month, trip_count DESC
        """

        client.query(query).result()
        print("Created payment_analysis table")

    @task(task_id="create_trip_distance_buckets")
    def create_trip_distance_buckets():
        """Create trip distance distribution analysis."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.trip_distance_distribution` AS
        SELECT
            CASE
                WHEN trip_distance_miles < 1 THEN '0-1 miles'
                WHEN trip_distance_miles < 2 THEN '1-2 miles'
                WHEN trip_distance_miles < 5 THEN '2-5 miles'
                WHEN trip_distance_miles < 10 THEN '5-10 miles'
                WHEN trip_distance_miles < 20 THEN '10-20 miles'
                ELSE '20+ miles'
            END AS distance_bucket,
            CASE
                WHEN trip_distance_miles < 1 THEN 1
                WHEN trip_distance_miles < 2 THEN 2
                WHEN trip_distance_miles < 5 THEN 3
                WHEN trip_distance_miles < 10 THEN 4
                WHEN trip_distance_miles < 20 THEN 5
                ELSE 6
            END AS bucket_order,

            COUNT(*) AS trip_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
            AVG(trip_duration_minutes) AS avg_duration_minutes,
            AVG(total_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            SUM(total_amount) AS total_revenue

        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
        WHERE NOT has_data_quality_issue
          AND trip_distance_miles >= 0
        GROUP BY distance_bucket, bucket_order
        ORDER BY bucket_order
        """

        client.query(query).result()
        print("Created trip_distance_distribution table")

    @task(task_id="create_vendor_comparison")
    def create_vendor_comparison():
        """Create vendor comparison analysis."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.vendor_comparison` AS
        WITH vendor_agg AS (
            SELECT
                EXTRACT(YEAR FROM pickup_date) AS year,
                EXTRACT(MONTH FROM pickup_date) AS month,
                FORMAT_DATE('%Y-%m', pickup_date) AS year_month,
                vendor_id,
                CASE vendor_id
                    WHEN 1 THEN 'Creative Mobile Technologies'
                    WHEN 2 THEN 'VeriFone'
                    ELSE 'Unknown'
                END AS vendor_name,

                COUNT(*) AS trip_count,
                SUM(passenger_count) AS total_passengers,
                SUM(trip_distance_miles) AS total_distance,
                SUM(total_amount) AS total_revenue,

                AVG(trip_distance_miles) AS avg_distance,
                AVG(trip_duration_minutes) AS avg_duration,
                AVG(total_amount) AS avg_fare,
                AVG(tip_amount) AS avg_tip

            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
            WHERE NOT has_data_quality_issue
            GROUP BY year, month, year_month, vendor_id
        )
        SELECT
            *,
            ROUND(trip_count * 100.0 / SUM(trip_count) OVER (PARTITION BY year, month), 2) AS market_share_pct
        FROM vendor_agg
        ORDER BY year, month, vendor_id
        """

        client.query(query).result()
        print("Created vendor_comparison table")

    @task(task_id="create_yoy_comparison")
    def create_yoy_comparison():
        """Create year-over-year comparison view."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.year_over_year` AS
        WITH yearly_stats AS (
            SELECT
                EXTRACT(YEAR FROM pickup_date) AS year,
                COUNT(*) AS total_trips,
                SUM(total_amount) AS total_revenue,
                SUM(tip_amount) AS total_tips,
                AVG(trip_distance_miles) AS avg_distance,
                AVG(trip_duration_minutes) AS avg_duration,
                AVG(total_amount) AS avg_fare
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.fct_trips`
            WHERE NOT has_data_quality_issue
            GROUP BY year
        )
        SELECT
            year,
            total_trips,
            total_revenue,
            total_tips,
            avg_distance,
            avg_duration,
            avg_fare,

            -- YoY changes
            LAG(total_trips) OVER (ORDER BY year) AS prev_year_trips,
            LAG(total_revenue) OVER (ORDER BY year) AS prev_year_revenue,
            ROUND((total_trips - LAG(total_trips) OVER (ORDER BY year)) * 100.0 / NULLIF(LAG(total_trips) OVER (ORDER BY year), 0), 2) AS trips_yoy_change_pct,
            ROUND((total_revenue - LAG(total_revenue) OVER (ORDER BY year)) * 100.0 / NULLIF(LAG(total_revenue) OVER (ORDER BY year), 0), 2) AS revenue_yoy_change_pct

        FROM yearly_stats
        ORDER BY year
        """

        client.query(query).result()
        print("Created year_over_year table")

    @task(task_id="log_gold_completion")
    def log_gold_completion():
        """Log completion of GOLD layer creation."""
        print("=" * 60)
        print("GOLD LAYER CREATION COMPLETE")
        print("=" * 60)
        print("Tables created:")
        print("  - daily_summary")
        print("  - hourly_patterns")
        print("  - monthly_trends")
        print("  - location_stats")
        print("  - payment_analysis")
        print("  - trip_distance_distribution")
        print("  - vendor_comparison")
        print("  - year_over_year")
        print("=" * 60)

    # DAG Flow
    dataset = create_gold_dataset()

    # Run aggregations in parallel after dataset creation
    daily = create_daily_summary()
    hourly = create_hourly_patterns()
    monthly = create_monthly_trends()
    locations = create_location_stats()
    payments = create_payment_analysis()
    distances = create_trip_distance_buckets()
    vendors = create_vendor_comparison()
    yoy = create_yoy_comparison()

    dataset >> [daily, hourly, monthly, locations, payments, distances, vendors, yoy] >> log_gold_completion()
