# 1. Chọn base image Airflow
FROM apache/airflow:3.1.0

# 2. Copy requirements vào image
COPY requirements.txt .

# 3. Cài các thư viện cho user airflow
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# 4. Quay về user mặc định (nếu muốn)
USER airflow
