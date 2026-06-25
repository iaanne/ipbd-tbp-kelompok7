# IPBD Stock Pipeline
**Real-time Ticker & Prediksi Pergerakan Harga Saham**

Proyek **Infrastruktur dan Platform Big Data** (IPBD) untuk menganalisis pergerakan IHSG dan memprediksi kapan menyentuh titik 6000 menggunakan pipeline big data end-to-end.

## Anggota Tim
| Nama | NIM | Peran |
|------|-----|-------|
| Adrian Farrel Aziz Yatyoga | L0224040 | Data Engineer, Infrastructure & ML Engineer  |
| Michael Christian Shan Geraldo | L0224035 | Analytics |

## Tujuan Bisnis
Memprediksi kapan IHSG benar-benar menyentuh titik 6000 (minimal 2 hari berturut-turut) dan melihat potensi market yang masih sehat untuk investasi.

## Struktur Folder
```
├── batch_processing/          # Scrape YFinance, Garage S3 CRUD, Spark Batch ETL
├── stream-processing/         # Kafka Producer + Spark Structured Streaming
├── ml_integration/            # Feature Eng, KMeans, LSTM, Prediksi
├── prefect/                   # Orchestration flows (pengganti Airflow)
├── dashboard_monitoring/      # Grafana, Prometheus, Alertmanager, PII masking
├── config/                    # Konfigurasi Garage, Trino, Hive, PostgreSQL
├── scripts/                   # Pipeline scripts (run_pipeline.sh dll)
├── docs/                      # Dokumentasi arsitektur & laporan
├── docker-compose.yml         # Semua service container
├── .env.example               # Contoh konfigurasi environment
└── README.md
```

## Prerequisites
- **Docker & Docker Compose** (untuk service: Garage, Kafka, Spark, Trino, dll)
- **Python 3.11+** (untuk pipeline scripts)
- Install dependencies: `pip install -r requirements.txt`

## Pipeline Stages

```
PHASE 0: SETUP INFRASTRUKTUR
  docker-compose up -d  →  Garage, Kafka, Spark, Trino, Prefect, Grafana, dll

PHASE 1: BATCH PIPELINE
  Scrape YFinance  →  Garage S3  →  Spark ETL (SMA, Volatility)

PHASE 2: ML PIPELINE
  Feature Engineering (18 fitur)  →  KMeans Clustering  →  LSTM Time Series

PHASE 3: STREAM PIPELINE (real-time)
  Kafka Producer  →  Spark Streaming  →  Real-time ML Prediction

PHASE 4: GOVERNANCE & MONITORING
  Data Quality  →  PII Masking  →  Grafana Dashboard  →  Alerting
```

---

## CARA CEPAT (Pakai .sh Script)

Semua script ada di folder `scripts/`. Jalankan dari root project:

### 1. Full Pipeline (Semua Sekaligus)
```bash
# Setup → Batch → ML → Data Quality → Summary
bash scripts/run_pipeline.sh
```

### 2. Batch Pipeline Saja
```bash
bash scripts/run_batch.sh
```

### 3. ML Pipeline Saja
```bash
bash scripts/run_ml.sh
```

### 4. Stream Processing (butuh 2 terminal)
```bash
# Terminal 1: Kafka Producer
bash scripts/run_stream.sh producer

# Terminal 2: Spark Streaming
bash scripts/run_stream.sh streaming

# Terminal 3 (opsional): Real-time ML Prediction
bash scripts/run_stream.sh realtime-ml
```

### 5. Demo 10x Run (Bukti ke Dosen)
```bash
# Batch pipeline jalan 10x otomatis + log
bash scripts/run_batch_demo.sh

# Full pipeline demo: Batch → DQ → PII → ML → Inventory
bash scripts/run_all_demo.sh
```

---

## CARA MANUAL (Step-by-Step)

### Phase 0: Setup Infrastructure
```bash
# 1. Clone & masuk folder
git clone git@github.com:iaanne/ipbd-tbp-kelompok7.git
cd ipbd-tbp-kelompok7

# 2. Copy environment
cp .env.example .env

# 3. Install dependencies
pip install -r requirements.txt

# 4. Jalankan semua service Docker
docker-compose up -d

# 5. Setup Garage S3 (bucket, key, policy)
bash config/garage/setup-garage.sh
```

### Phase 1: Batch Pipeline
```bash
# Step 1: Scrape data historis dari YFinance (6 saham)
cd batch_processing
python scrape_historical.py
# Output: data/saham_indonesia_historical.csv

# Step 2: Upload CSV ke Garage S3 (buat bucket + upload)
python minio_operations.py --keep-data

# Step 3: Spark Batch ETL (hitung SMA 7/30, Volatility, Price Range)
python spark_batch_etl.py
# Output: s3://stock-bucket/processed-data/features/

# Step 4 (opsional): Data Quality Check
cd ../dashboard_monitoring
python data_quality.py
```

### Phase 2: ML Pipeline
```bash
# Step 1: Feature Engineering (18 fitur teknikal)
cd ml_integration
python feature_engineering.py
# Output: s3://stock-bucket/features/

# Step 2: Training KMeans Clustering (4 cluster risiko)
python train_model.py
# Output: Model + predictions ke s3://stock-bucket/models/ & predictions/

# Step 3: Batch Inference + Enrich Data
python batch_inference.py
# Output: Enriched predictions + IHSG analysis (estimasi hari ke 6000)

# Step 4: Training LSTM Time Series
python lstm_train.py
# Output: Model .h5 ke s3://stock-bucket/models/ + MLflow tracking

# Step 5: LSTM Prediksi + Estimasi Hari ke 6000
python lstm_predict.py
# Output: Prediksi 7 hari ke depan + estimasi kapan IHSG tembus 6000
```

### Phase 3: Stream Processing (Real-time)
```bash
# Terminal 1: Kafka Producer (stream data real-time dari YFinance)
cd stream-processing
python kafka_producer.py

# Terminal 2: Spark Streaming (aggregate 5 menit dari Kafka)
python spark_streaming.py

# Terminal 3 (opsional): Real-time ML Prediction dari Kafka
cd ../ml_integration
python realtime_prediction.py
```

### Phase 4: Data Governance
```bash
# PII Masking Demo (Email, Nama, Telepon, Alamat, Saldo)
cd dashboard_monitoring
python masking_pii.py
# Output: Original + masked CSV ke s3://stock-bucket/pii-sample/
```

### Register Prefect Orchestration (Opsional)
```bash
# Batch pipeline (setiap hari kerja jam 18:00)
prefect deployment build prefect/flows/batch_pipeline.py:batch_flow \
  --name "Batch Pipeline" --cron "0 18 * * 1-5"

# ML Training (Senin/Rabu/Jumat 06:00)
prefect deployment build prefect/flows/ml_training.py:ml_training_flow \
  --name "ML Training" --cron "0 6 * * 1,3,5"

# Data Quality (setiap hari kerja 07:00)
prefect deployment build prefect/flows/data_quality_flow.py:data_quality_flow \
  --name "Data Quality" --cron "0 7 * * 1-5"
```

---

## Access Services
| Service | URL | Login |
|---------|-----|-------|
| Garage WebUI | http://localhost:3909 | admin_token_rahasia123 |
| Spark UI | http://localhost:8080 | - |
| Prefect UI | http://localhost:4200 | - |
| Trino SQL | http://localhost:8082 | - |
| MLflow | http://localhost:5000 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| InfluxDB | http://localhost:8086 | - |

## Tech Stack
- **Data Source**: YFinance, IDX
- **Storage**: Garage S3 (S3-compatible, Rust-based)
- **Streaming**: Apache Kafka, Spark Structured Streaming
- **Batch**: Apache Spark (Hadoop S3A)
- **Query Engine**: Trino + Hive Metastore
- **ML Lifecycle**: MLflow (Tracking, Model Registry, Artifacts)
- **ML Models**: KMeans Clustering (risk profiling) + LSTM (price prediction)
- **Orchestration**: Prefect (Flows + Agent)
- **Monitoring**: Prometheus, InfluxDB, Grafana, Alertmanager
- **Notification**: Email (SMTP), Telegram (Bot API)

## PII Masking
Script `dashboard_monitoring/masking_pii.py` mendemonstrasikan masking data pribadi:
- Email → partial masking (b****@gmail.com)
- Nama → partial masking (Budi S***)
- Telepon → partial masking (0812****890)
- Alamat → generalize (Jalan ***** No.X, Kota)
- Saldo → kategorisasi (< 50jt, 50-100jt, > 100jt)
- Data fiktif disimpan di `stock-bucket/pii-sample/`

## Keamanan
- Password & credentials via environment variable (`.env`)
- Data publik YFinance tidak mengandung PII
- Audit trail semua operasi pipeline di `audit/`
- Bucket policy Garage S3 untuk access control
