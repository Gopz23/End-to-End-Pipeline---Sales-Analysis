from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime

PROJECT_ID = "your-project-id"
REGION = "us-central1"
CLUSTER = "sales-cluster"

JOB = {
    "placement": {"cluster_name": CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": "gs://your-bucket-name/pyspark_job.py"
    },
}

with DAG(
    "sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_job = DataprocSubmitJobOperator(
        task_id="run_pyspark",
        job=JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    run_job
