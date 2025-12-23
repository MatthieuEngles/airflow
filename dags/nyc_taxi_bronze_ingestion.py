"""
NYC Yellow Cab Data Ingestion DAG - BRONZE Layer

Downloads Yellow Taxi trip data from NYC TLC and stores raw parquet files in GCS.
Data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

This DAG processes the entire historical dataset from 2009 to present.
"""

from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

# Configuration from environment
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'data-curriculum-478510')
GCS_BUCKET_BRONZE = os.environ.get('GCS_BUCKET_BRONZE', 'data-curriculum-478510-nyc-taxi-bronze')

# NYC TLC data is available from January 2009
# Starting from January 2024
START_DATE = datetime(2024, 1, 1)

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="nyc_taxi_bronze_ingestion",
    description="Ingest NYC Yellow Cab data from TLC to GCS (BRONZE layer)",
    start_date=START_DATE,
    schedule="@monthly",
    catchup=True,  # Process all historical data
    max_active_runs=3,  # Limit concurrent runs to avoid overwhelming resources
    default_args=default_args,
    tags=["nyc-taxi", "bronze", "ingestion"],
) as dag:

    @task(task_id="generate_download_url")
    def generate_download_url(logical_date: datetime) -> dict:
        """Generate the TLC download URL for the given month."""
        year = logical_date.year
        month = logical_date.month

        # NYC TLC parquet file URL pattern
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

        gcs_path = f"gs://{GCS_BUCKET_BRONZE}/yellow_taxi/{year}/{month:02d}/{filename}"

        return {
            "url": url,
            "filename": filename,
            "gcs_path": gcs_path,
            "year": year,
            "month": month,
        }

    @task(task_id="download_and_upload_to_gcs")
    def download_and_upload_to_gcs(url_info: dict) -> dict:
        """Download parquet file from TLC and upload to GCS."""
        from google.cloud import storage
        import requests
        import tempfile
        import os

        url = url_info["url"]
        filename = url_info["filename"]
        year = url_info["year"]
        month = url_info["month"]

        print(f"Downloading {filename} from {url}")

        # Download with streaming to handle large files
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        # Get file size for logging
        file_size = int(response.headers.get('content-length', 0))
        print(f"File size: {file_size / (1024*1024):.2f} MB")

        # Save to temp file first
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            tmp_path = tmp_file.name
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
                downloaded += len(chunk)
            print(f"Downloaded {downloaded / (1024*1024):.2f} MB to {tmp_path}")

        # Upload to GCS
        try:
            client = storage.Client(project=GCP_PROJECT_ID)
            bucket = client.bucket(GCS_BUCKET_BRONZE)
            blob_path = f"yellow_taxi/{year}/{month:02d}/{filename}"
            blob = bucket.blob(blob_path)

            print(f"Uploading to gs://{GCS_BUCKET_BRONZE}/{blob_path}")
            blob.upload_from_filename(tmp_path)
            print(f"Upload complete!")

            return {
                "gcs_uri": f"gs://{GCS_BUCKET_BRONZE}/{blob_path}",
                "year": year,
                "month": month,
                "file_size_mb": round(downloaded / (1024*1024), 2),
                "status": "success"
            }
        finally:
            # Clean up temp file
            os.unlink(tmp_path)

    @task(task_id="validate_upload")
    def validate_upload(upload_result: dict) -> dict:
        """Validate the uploaded file exists and has correct size."""
        from google.cloud import storage
        import pyarrow.parquet as pq
        from io import BytesIO

        gcs_uri = upload_result["gcs_uri"]
        print(f"Validating {gcs_uri}")

        client = storage.Client(project=GCP_PROJECT_ID)

        # Parse GCS URI
        bucket_name = gcs_uri.replace("gs://", "").split("/")[0]
        blob_path = "/".join(gcs_uri.replace("gs://", "").split("/")[1:])

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise ValueError(f"File not found: {gcs_uri}")

        # Get blob metadata
        blob.reload()
        size_mb = blob.size / (1024 * 1024)
        print(f"File exists with size: {size_mb:.2f} MB")

        # Read parquet metadata to get row count
        content = blob.download_as_bytes()
        parquet_file = pq.ParquetFile(BytesIO(content))
        num_rows = parquet_file.metadata.num_rows
        num_columns = parquet_file.metadata.num_columns

        print(f"Parquet file contains {num_rows:,} rows and {num_columns} columns")

        return {
            **upload_result,
            "validated": True,
            "num_rows": num_rows,
            "num_columns": num_columns,
        }

    @task(task_id="log_ingestion_metrics")
    def log_ingestion_metrics(validation_result: dict):
        """Log ingestion metrics for monitoring."""
        print("=" * 60)
        print("BRONZE INGESTION COMPLETE")
        print("=" * 60)
        print(f"Year/Month: {validation_result['year']}/{validation_result['month']:02d}")
        print(f"GCS URI: {validation_result['gcs_uri']}")
        print(f"File Size: {validation_result['file_size_mb']} MB")
        print(f"Row Count: {validation_result['num_rows']:,}")
        print(f"Column Count: {validation_result['num_columns']}")
        print("=" * 60)
        return validation_result

    # DAG Flow
    url_info = generate_download_url()
    upload_result = download_and_upload_to_gcs(url_info)
    validation_result = validate_upload(upload_result)
    log_ingestion_metrics(validation_result)
