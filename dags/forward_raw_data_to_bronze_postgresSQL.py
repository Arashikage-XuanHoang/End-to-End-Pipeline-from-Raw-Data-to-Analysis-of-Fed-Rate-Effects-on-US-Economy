from airflow.decorators import dag, task
import pendulum
import os
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ----------------------
# Config
MINIO_CONN_ID = os.getenv("MINIO_CONN_ID", "minio_conn")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_localhost")
BUCKET_NAME = "raw-data"
SCHEMA = "bronze"

# ----------------------
# DAG
# ----------------------
@dag(
    dag_id="dynamic_minio_to_postgres_bronze_v3",
    start_date=pendulum.datetime(2025, 10, 16),
    schedule=None,
    catchup=False,
    tags=["bronze", "minio", "postgres", "dynamic-tasks"],
)
def forward_to_postgres():

    @task
    def list_csv_files_from_minio() -> list[str]:
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        files = [key for key in s3_hook.list_keys(bucket_name=BUCKET_NAME) if key.endswith(".csv")]
        print(f"Tìm thấy {len(files)} file CSV: {files}")
        return files

    @task
    def upload_single_csv_to_bronze(file_key: str):
        print(f"--- DEBUG: file_key = '{file_key}' ---")
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        obj = s3_hook.get_key(key=file_key, bucket_name=BUCKET_NAME)
        df = pd.read_csv(obj.get()['Body'], dtype=str)
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        table_name = file_key.replace('.csv', '').lower() + "_raw"
        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists='replace',
            index=False
        )
        print(f"Đã tải thành công {file_key} vào bảng {SCHEMA}.{table_name}")

    # Chain task
    files = list_csv_files_from_minio()
    upload_single_csv_to_bronze.expand(file_key=files)

dag_instance = forward_to_postgres()
