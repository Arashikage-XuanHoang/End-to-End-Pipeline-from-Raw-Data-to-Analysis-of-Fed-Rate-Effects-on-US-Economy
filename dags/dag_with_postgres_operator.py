from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Default arguments
default_args = {
    'owner': 'xuanhoang',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag_create_table_postgres',
    default_args=default_args,
    description='Ví dụ tạo bảng trong PostgreSQL từ Airflow',
    start_date=datetime(2025, 10, 10),
    schedule = '@daily',  # chỉ chạy thủ công
    catchup=False,
    tags=['postgres', 'example']
) as dag:

    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',  # ID này phải tồn tại trong Airflow Connections
        sql="""
        CREATE TABLE IF NOT EXISTS dag_runs (
            dt DATE,
            dag_id VARCHAR(100),
            PRIMARY KEY (dt, dag_id)
        );
        """
    )

    create_table
