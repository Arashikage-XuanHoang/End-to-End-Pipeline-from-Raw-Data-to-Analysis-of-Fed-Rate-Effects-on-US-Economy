# dags/read_economics_csv.py
from airflow.decorators import dag, task
from datetime import datetime
import os

# Dùng try để đảm bảo không lỗi nếu pandas chưa cài
try:
    import pandas as pd
except ImportError:
    pd = None


@dag(
    dag_id="read_economics_csv",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # chỉ chạy thủ công
    catchup=False,
    tags=["example", "csv", "local"],
)
def read_economics_csv():
    @task()
    def read_csv_file():
        file_path = "/opt/airflow/data/Economics.csv"
        print(f"📂 Checking file: {file_path}")

        # Kiểm tra file có tồn tại không
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"⚠️ File is not readable: {file_path}")

        # Nếu pandas chưa cài, cảnh báo thay vì lỗi
        if pd is None:
            print("⚠️ pandas is not installed inside the container.")
            print("You can install it by adding this line to docker-compose.yml:")
            print('  _PIP_ADDITIONAL_REQUIREMENTS: "pandas"')
            return {"error": "pandas not installed"}

        # Đọc CSV
        df = pd.read_csv(file_path)
        print("✅ File loaded successfully!")
        print(df.head())
        print(f"\nTotal rows: {len(df)}")

        return {
            "columns": df.columns.tolist(),
            "row_count": len(df),
        }

    read_csv_file()


dag = read_economics_csv()
