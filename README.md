# End-to-End Pipeline: Fed Rate Analysis 🇺🇸💹

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.x-orange)
![Docker](https://img.shields.io/badge/Docker-✓-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-✓-blue)
![MinIO](https://img.shields.io/badge/MinIO-✓-green)

---

## 📌 Project Overview
Pipeline **end-to-end** sử dụng **Airflow** để:
1. Upload raw CSV → MinIO → PostgreSQL (Bronze layer)  
2. Process & merge data (Silver layer)  
3. Create dimension & fact tables (Gold layer) → ready for analysis  

Mục tiêu: phân tích **ảnh hưởng của Fed Funds Rate** tới nền kinh tế Mỹ.

---

## 🗂 Repo Structure

Airflow_docker/
├── dags/ # Airflow DAGs
├── config/ # Airflow configuration
├── docker-compose.yaml # Local deployment
├── requirements.txt # Python dependencies
├── .gitignore
└── README.md


---

## ⚙️ Setup & Run

### 1️⃣ Clone repo
```bash
git clone https://github.com/Arashikage-XuanHoang/<Tên-repo>.git
cd <Tên-repo>
### 2️⃣ Start services
docker-compose up -d
Airflow UI: http://localhost:8080

MinIO: http://localhost:9000

PostgreSQL: localhost:5432

### 🔍 Key Features

Bronze Layer: Raw data ingestion

Silver Layer: Data cleaning & merge

Gold Layer: Dimensional & fact tables for analysis

Fully containerized: Airflow + Postgres + MinIO

Secrets safe: .env ignored

