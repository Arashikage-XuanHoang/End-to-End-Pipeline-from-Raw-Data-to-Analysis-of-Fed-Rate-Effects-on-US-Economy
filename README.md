# End-to-End Pipeline: Phân tích ảnh hưởng của Fed Funds Rate lên nền kinh tế Hoa Kỳ

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.x-orange)
![Docker](https://img.shields.io/badge/Docker-✓-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-✓-blue)
![MinIO](https://img.shields.io/badge/MinIO-✓-green)

---

## 📌 Tổng quan dự án

Dự án này xây dựng một pipeline dữ liệu **end-to-end** hoàn chỉnh sử dụng **Apache Airflow** để tự động hóa quy trình thu thập, xử lý và mô hình hóa dữ liệu.

Mục tiêu cuối cùng là chuẩn bị dữ liệu sạch, có cấu trúc để phục vụ cho việc phân tích **ảnh hưởng của Lãi suất Quỹ Liên bang (Fed Funds Rate)** đến các chỉ số kinh tế quan trọng của Mỹ.

Pipeline được chia làm 3 giai đoạn chính theo kiến trúc Medallion:
1.  **Bronze Layer**: Tải dữ liệu thô (CSV) lên MinIO và sau đó vào PostgreSQL.
2.  **Silver Layer**: Xử lý, làm sạch và kết hợp các nguồn dữ liệu.
3.  **Gold Layer**: Tạo các bảng dimension và fact, sẵn sàng cho việc phân tích hoặc đưa vào các công cụ BI.

---

## 🛠️ Công nghệ sử dụng

* **Orchestration**: Apache Airflow
* **Containerization**: Docker & Docker Compose
* **Data Lake Storage**: MinIO
* **Data Warehouse**: PostgreSQL
* **Language**: Python

---

## 🗂️ Cấu trúc thư mục

Để tạo ra cấu trúc cây như bên dưới, bạn chỉ cần đặt văn bản đã định dạng vào trong một khối code (sử dụng dấu ```).

```
.
├── dags/                  # Nơi chứa các file DAGs của Airflow
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   └── gold_layer.py
├── config/                # Chứa các file cấu hình của Airflow (airflow.cfg)
├── docker-compose.yaml    # File định nghĩa các service để chạy local (Airflow, Postgres, MinIO)
├── requirements.txt       # Danh sách các thư viện Python cần thiết
├── .gitignore             # Các file và thư mục được Git bỏ qua
└── README.md              # File giới thiệu dự án (chính là file này)
```

---

## 🚀 Hướng dẫn cài đặt và chạy dự án

1.  **Clone repository:**
    ```bash
    git clone https://github.com/Arashikage-XuanHoang/End-to-End-Pipeline-from-Raw-Data-to-Analysis-of-Fed-Rate-Effects-on-US-Economy
    cd Airflow_docker
    ```

2.  **Khởi chạy các service với Docker Compose:**
    ```bash
    docker-compose up -d
    ```
    Lệnh này sẽ khởi tạo các container cho Airflow (webserver, scheduler, worker), PostgreSQL và MinIO.

3.  **Truy cập Airflow UI:**
    Mở trình duyệt và truy cập vào địa chỉ: `http://localhost:8080`
    * **Username**: `airflow`
    * **Password**: `airflow`

4.  **Kích hoạt DAGs:**
    Trên giao diện Airflow, bạn sẽ thấy các DAGs đã được định nghĩa trong thư mục `dags/`. Bật các DAGs để bắt đầu chạy pipeline.

5.  **Key Features:**
    * **Bronze Layer**: Lưu dữ liệu raw từ CSV → MinIO → Postgres
    * **Silver Layer**: Xử lý, merge dữ liệu
    * **Gold Layer**: Tạo dimension & fact tables cho phân tích
    * **Fully containerized**: Airflow + Postgres + MinIO
    * **Secrets được bảo vệ**: .env đã được ignore
