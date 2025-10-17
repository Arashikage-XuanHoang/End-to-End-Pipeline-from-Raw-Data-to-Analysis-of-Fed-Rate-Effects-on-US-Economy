from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os


MINIO_CONN_ID = os.getenv("MINIO_CONN_ID", "minio_conn")  # Tên connection trong Airflow
BUCKET_NAME = os.getenv("MINIO_BUCKET", "raw-data")       # Tên bucket
DATA_DIR = os.getenv("LOCAL_DATA_DIR", "/opt/airflow/data")  # Thư mục mount local


def upload_local_files_to_minio():
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    files = ['Economics.csv', 'fomc_meeting_2000_2025.csv']

    for f in files:
        path = os.path.join(DATA_DIR, f)
        if os.path.exists(path):
            hook.load_file(filename=path, key=f, bucket_name=BUCKET_NAME, replace=True)
            print(f"Uploaded {f}")
        else:
            print(f"File không tồn tại: {path}")

with DAG(
    dag_id = 'upload_local_to_minio_soa',
    start_date=datetime(2025, 10, 15),
    schedule= "@daily",
    # catchup=False,
    tags=['minio', 'local', 'upload', 'data_lake', 'raw_data'],
) as dag:
    upload_task = PythonOperator(
        task_id='upload_local_files',
        python_callable=upload_local_files_to_minio
    )

upload_task
