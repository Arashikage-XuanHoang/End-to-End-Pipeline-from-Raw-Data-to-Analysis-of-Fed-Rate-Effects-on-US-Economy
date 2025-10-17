from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd

default_args = {
    'owner': 'xuanhoang',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
def read_first_rows():
    file_path = "/opt/airflow/data/Economics.csv"
    df = pd.read_csv(file_path)
    print(f"Kích thước file: {df.shape}")
    print(df.head())
with DAG(
    dag_id='read_header_csv_kkoke',
    default_args=default_args,
    description='Upload local CSV to /opt/airflow/data in container',
    start_date=datetime(2025, 10, 12),
    schedule = "* * * * *",  # chạy thủ công
    catchup=False,
    tags=['IS217']
) as dag:
    task_read_csv = PythonOperator(
        task_id = 'read_csv_Economics',
        python_callable = read_first_rows
    )
    task_read_csv