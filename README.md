# ipbd-stock-pipeline-kelompok7

Project IPBD (Infrastruktur dan Platform Big Data) – Real-time Ticker & Prediksi Pergerakan Harga Saham Indonesia

Repo ini merupakan monorepo yang menggabungkan **Batch Processing**, **Stream Processing**, **Machine Learning Integration**, **Dashboard & Monitoring**, dan **MinIO Object Storage** untuk pipeline big data harga saham index lokal Indonesia (IDX).

## 📁 Struktur Folder
├── 1-batch-processing/       # Batch Processing Layer (Spark, MinIO)
│   ├── scrape_historical.py      # Scraping data historis Yahoo Finance
│   ├── minio_operations.py     # CRUD MinIO bucket (create, upload, delete)
│   ├── spark_batch_etl.py      # Spark Batch ETL: transformasi & indikator teknikal
│   └── data/                        # Folder data CSV (di .gitignore)
│
├── 2-stream-processing/      # Stream Processing Layer (Kafka, Spark Streaming)
│   ├── kafka_producer.py       # Simulasi data real-time ke Kafka topic
│   ├── spark_streaming.py      # Spark Structured Streaming: Kafka → process
│   └── checkpoints/                 # Spark checkpoint (di .gitignore)
│
├── 3-ml-integration/         # ML Integration Layer (Feature Eng, Training, Inference)
│   ├── feature_engineering.py  # Ekstraksi fitur dari data processed
│   ├── train_model.py          # Training model prediksi (Spark MLlib / Scikit-learn)
│   ├── batch_inference.py       # Prediksi batch untuk data historis
│   └── realtime_prediction.py  # Prediksi real-time via API
│
├── 4-dashboard-monitoring/   # Dashboard & Monitoring (Grafana, Prometheus, Alertmanager)
│   ├── grafana-dashboards/
│   │   └── stock-dashboard.json     # Template dashboard Grafana
│   ├── prometheus/
│   │   └── prometheus.yml           # Config Prometheus scraping
│   ├── alertmanager/
│   │   └── alertmanager.yml         # Config alert notification
│   └── docker-compose.override.yml  # Override untuk monitoring stack (opsional)
│
├── 5-docs/                   # Dokumentasi & Screenshot untuk Laporan PDF
│   ├── screenshots/
│   │   ├── ss_1_minio_login.png
│   │   ├── ss_2_minio_dashboard.png
│   │   ├── ss_3_create_bucket.png
│   │   ├── ss_4_upload_csv.png
│   │   └── ss_5_delete_bucket.png
│   └── laporan/
│       └── template_laporan.md      # Template markdown laporan
│
├── 6-config/               # Config files (Keamanan & Environment)
│   ├── .env.example                 # Template environment variables
│   └── minio-policy.json            # Bucket policy MinIO (auth & authorization)
│
├── docker-compose.yml        # Semua service: MinIO, Kafka, Spark, Grafana, Prometheus, InfluxDB
├── requirements.txt          # Dependency Python (yfinance, pandas, boto3, pyspark, kafka-python, scikit-learn)
├── .gitignore                # File & folder yang di-exclude dari git
└── README.md                 


## 👥 Anggota Kelompok & Pembagian Tugas

| No | Nama | NIM | Fokus | Aspek Penilaian |
|----|------|-----|-------|------------------|
| 1 | Adrian Farrel Aziz Yatyoga | L0224040 | **Data Engineer & Infrastructure** | Batch Processing, Stream Processing, MinIO, Kafka, Spark, Keamanan Data |
| 2 | Michael Christian Shan Geraldo | L0224035 | **ML Engineer & Analytics** | ML Integration, Dashboard Grafana, Monitoring & Logging, Alerting, Governance |

### Pembagian Detail

**👤 Ian – Data Engineer & Infrastructure:**
-  Perancangan Arsitektur Pipeline (desain end-to-end)
-  Batch Processing (Spark Batch ETL: SMA, Volatility, indikator teknikal)
-  Stream Processing (Kafka + Spark Structured Streaming)
-  MinIO Operations (create bucket, upload CSV, delete bucket + screenshot)
-  Keamanan Data (MinIO auth, bucket policies, access control)

**👤 Aldo – ML Engineer & Analytics:**
-  Integrasi Machine Learning (feature engineering, training, batch inference, real-time prediction API)
-  Visualisasi & Dashboard (Grafana: real-time charts, ML predictions, stock trends)
-  Monitoring & Logging (Prometheus metrics, pipeline performance tracking)
-  Alerting System (Grafana alert rules, Alertmanager, anomaly notifications)
-  Governance Big Data (data quality checks, metadata, audit trail, compliance)

## 🚀 Cara Menjalankan

### 1. Setup Environment

```bash
# Clone repo
git clone <URL_REPO_KALIAN>
cd ipbd-stock-pipeline-kelompok7

# Copy environment config
cp 6-config/.env.example .env

# Install Python dependencies
pip install -r requirements.txt

# Jalankan semua service: MinIO, Kafka, Spark, Grafana, Prometheus, InfluxDB
docker-compose up -d

# Cek status container
docker-compose ps



cd 1-batch-processing

# Step 1: Scraping data historis dari Yahoo Finance
python scrape_historical.py

# Step 2: Upload CSV ke MinIO (screenshot untuk PDF!)
python minio_operations.py

# Step 3: Spark Batch ETL (baca dari MinIO → transform → save ke MinIO)
python spark_batch_etl.py


cd 2-stream-processing

# Terminal 1: Jalankan Kafka Producer (simulasi data real-time)
python 2a_kafka_producer.py

# Terminal 2: Jalankan Spark Streaming (baca dari Kafka)
python 2b_spark_streaming.py


cd 3-ml-integration

# Step 1: Feature Engineering (baca processed data dari MinIO)
python 3a_feature_engineering.py

# Step 2: Training Model
python 3b_train_model.py

# Step 3: Batch Inference (prediksi data historis)
python 3c_batch_inference.py

# Step 4: Real-time Prediction API (FastAPI)
python 3d_realtime_prediction.py


# Import dashboard template ke Grafana
# 1. Buka http://localhost:3000
# 2. Login: admin / admin
# 3. Dashboards → Import → Upload 4-dashboard-monitoring/grafana-dashboards/stock-dashboard.json

# Alert rules sudah di-config di prometheus.yml dan alertmanager.yml

