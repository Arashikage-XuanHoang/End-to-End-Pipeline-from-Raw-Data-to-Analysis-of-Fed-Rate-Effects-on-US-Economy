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

