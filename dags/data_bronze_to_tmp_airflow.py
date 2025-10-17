from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import os
import boto3

# ------------------------
# Config
# ------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "raw-data")
FILES = ["Economics.csv", "fomc_meeting_2000_2025.csv"]

# ------------------------
# Function
# ------------------------
def extract_from_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    for f in FILES:
        s3.download_file(BUCKET_NAME, f, f"/tmp/{f}")
        print(f"Downloaded {f} to /tmp/{f}")

# ------------------------
# DAG
# ------------------------
with DAG(
    dag_id="extract_from_minio",
    start_date=datetime(2025, 10, 12),
    schedule=None,
    catchup=False,
    tags=["extract", "download"]
) as dag:

    task_extract = PythonOperator(
        task_id="extract_from_minio",
        python_callable=extract_from_minio
    )
