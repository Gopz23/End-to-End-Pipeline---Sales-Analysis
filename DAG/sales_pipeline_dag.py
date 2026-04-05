from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import yaml
import os

# Correct absolute path handling
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "../configs/config.yaml")

# Load config.yaml
with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# Config values (REAL)
PROJECT_ID = "gopz-sales-pipeline-2026"
REGION = "asia-south1"
CLUSTER_NAME = "gopz-sales-dataproc-cluster"
BUCKET_NAME = "gopz-sales-data-bucket-2026"

# Dataproc Job Configuration
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://gopz-sales-data-bucket-2026/dataproc/pyspark_job.py"
    },
}

# Default arguments
default_args = {
    "owner": "gopz",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# DAG Definition
with DAG(
    dag_id="gcp_sales_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["gcp", "dataproc", "sales"]
) as dag:

    run_pyspark_job = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    run_pyspark_job
