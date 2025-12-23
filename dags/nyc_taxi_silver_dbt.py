"""
NYC Yellow Cab DBT Transformation DAG - SILVER Layer

Runs dbt models to transform BRONZE data into SILVER layer.
Creates dimension tables and fact tables in BigQuery.
"""

from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt/nyc_taxi"

with DAG(
    dag_id="nyc_taxi_silver_dbt",
    description="Run dbt transformations for SILVER layer",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi", "silver", "dbt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Create external table for bronze data
    @task(task_id="create_external_table")
    def create_external_table():
        """Create BigQuery external table pointing to GCS parquet files."""
        from google.cloud import bigquery
        from google.cloud import storage
        import os

        project_id = os.environ.get('GCP_PROJECT_ID', 'data-curriculum-478510')
        bucket_name = os.environ.get('GCS_BUCKET_BRONZE', 'data-curriculum-478510-nyc-taxi-bronze')

        # List all parquet files in GCS to build source URIs
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="yellow_taxi/")

        source_uris = []
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                source_uris.append(f"gs://{bucket_name}/{blob.name}")

        if not source_uris:
            raise ValueError(f"No parquet files found in gs://{bucket_name}/yellow_taxi/")

        print(f"Found {len(source_uris)} parquet files")

        client = bigquery.Client(project=project_id)

        # Create dataset if not exists
        dataset_id = f"{project_id}.nyc_taxi_bronze"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)

        # Create external table
        table_id = f"{dataset_id}.yellow_taxi_external"

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = source_uris
        external_config.autodetect = True

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        # Delete if exists and recreate
        client.delete_table(table_id, not_found_ok=True)
        table = client.create_table(table)

        print(f"Created external table {table_id}")
        return table_id

    # Run all dbt models (staging -> dimensions -> facts in dependency order)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
    )

    @task(task_id="log_completion")
    def log_completion():
        """Log completion of SILVER transformations."""
        print("=" * 60)
        print("SILVER LAYER TRANSFORMATION COMPLETE")
        print("=" * 60)
        print("Dimension tables: dim_vendor, dim_payment_type, dim_rate_code, dim_date, dim_time")
        print("Fact tables: fct_trips")
        print("=" * 60)

    # DAG Flow
    external_table = create_external_table()

    dbt_deps >> dbt_seed >> external_table >> dbt_run >> dbt_test >> log_completion()
