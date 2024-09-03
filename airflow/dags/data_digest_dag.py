import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Updated dataset file and URL to point to the gzipped file
dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = "https://raw.githubusercontent.com/BishoyAbd/data-eng-zoomcamp/main/yellow_tripdata_2021-01.csv.gz"

# File paths
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
compressed_file = f"{dataset_file}.gz"
extracted_file = dataset_file
parquet_file = dataset_file.replace('.csv', '.parquet')

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'my_default_dataset')

def format_to_parquet(src_file):
    """Converts the CSV file to Parquet format."""
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """Uploads the file to Google Cloud Storage."""
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    # Download the dataset
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{compressed_file}"
    )


    # Extract the dataset
    extract_dataset_task = BashOperator(
        task_id="extract_dataset_task",
        bash_command=f"gunzip -c {path_to_local_home}/{compressed_file} > {path_to_local_home}/{extracted_file}"
    )

    # Convert to Parquet format
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{extracted_file}",
        },
    )

    # Upload to GCS
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    # Create an external table in BigQuery
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    # # Set task dependencies
    download_dataset_task >> extract_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
