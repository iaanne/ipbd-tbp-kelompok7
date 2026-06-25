# IPBD Stock Pipeline - Real-time Ticker & Prediksi Pergerakan Harga Saham

Proyek **Infrastruktur dan Platform Big Data** (IPBD) untuk menganalisis pergerakan IHSG dan memprediksi kapan menyentuh titik 6000 menggunakan pipeline big data end-to-end.

## Tujuan Bisnis
Memprediksi kapan IHSG benar-benar menyentuh titik 6000 (minimal 2 hari berturut-turut) dan melihat potensi market yang masih sehat untuk investasi.

## Anggota Tim
| Nama | NIM | Peran |
|------|-----|-------|
| Adrian Farrel Aziz Yatyoga | L0224040 | Data Engineer & Infrastructure |
| Michael Christian Shan Geraldo | L0224035 | ML Engineer & Analytics |

## Struktur Folder
```
├── batch_processing/          # Batch Processing (scrape, Garage S3, Spark ETL)
│   ├── scrape_historical.py   # Scraping YFinance
│   ├── garage_operations.py   # CRUD Garage S3 (ex-minio_operations)
│   ├── spark_batch_etl.py     # Spark ETL: SMA, Volatility, dll.
│   └── data/                  # Data CSV
├── stream-processing/         # Stream Processing
│   ├── kafka_producer.py      # Kafka producer real-time
│   └── spark_streaming.py     # Spark Structured Streaming
├── ml_integration/            # ML Pipeline
│   ├── feature_engineering.py # 18 fitur teknis
│   ├── train_model.py         # KMeans clustering
│   ├── batch_inference.py     # Batch prediction + enrich
│   ├── realtime_prediction.py # Real-time prediction
│   ├── lstm_train.py          # LSTM time series training
│   └── lstm_predict.py        # LSTM prediction + IHSG 6000 estimator
├── prefect/                   # Orchestration (pengganti Airflow)
│   ├── flows/
│   │   ├── batch_pipeline.py
│   │   ├── ml_training.py
│   │   ├── data_quality_flow.py
│   │   ├── stream_monitor.py
│   │   ├── push_metrics.py
│   │   └── alert_notify.py
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard_monitoring/      # Monitoring & Dashboard
│   ├── prometheus/
│   ├── alertmanager/
│   ├── grafana-dashboards/
│   ├── data_quality.py
│   └── masking_pii.py
├── config/                    # Konfigurasi Infrastruktur
│   ├── garage/garage.toml
│   ├── trino/
│   ├── hive/
│   └── postgresql/
├── scripts/                   # Demo scripts
│   ├── run_batch_demo.sh      # Batch 10x auto-run
│   └── run_all_demo.sh        # Full pipeline demo
├── docs/                      # Dokumentasi
│   ├── ARCHITECTURE.md        # Diagram arsitektur Mermaid
│   └── REPORT.md              # Laporan lengkap
├── docker-compose.yml         # Semua service container
├── .env.example
└── README.md
```

## Arsitektur
Lihat [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) untuk diagram arsitektur lengkap.

## Prerequisites
- Docker & Docker Compose
- Python 3.11+

## Pembagian Detail
**Ian – Data Engineer & Infrastructure:**
- Perancangan Arsitektur Pipeline (desain end-to-end)
- Batch Processing (Spark Batch ETL: SMA, Volatility, indikator teknikal)
- Stream Processing (Kafka + Spark Structured Streaming)
- MinIO Operations (create bucket, upload CSV, delete bucket + screenshot)
- Keamanan Data (MinIO auth, bucket policies, access control)

**Aldo – ML Engineer & Analytics:**
- Integrasi Machine Learning (feature engineering, training, batch inference, real-time prediction API)
- Visualisasi & Dashboard (Grafana: real-time charts, ML predictions, stock trends)
- Monitoring & Logging (Prometheus metrics, pipeline performance tracking)
- Alerting System (Grafana alert rules, Alertmanager, anomaly notifications)
- Governance Big Data (data quality checks, metadata, audit trail, compliance)

## Cara Menjalankan

### 1. Setup Infrastructure
```bash
# Clone repo
git clone git@github.com:iaanne/ipbd-tbp-kelompok7.git
cd ipbd-tbp-kelompok7

# Copy environment config
cp .env.example .env

# Install Python dependencies
pip install -r requirements.txt

# Jalankan semua service
docker-compose up -d
```

### 2. Access Services
| Service | URL | Login |
|---------|-----|-------|
| Garage WebUI | http://localhost:3909 | admin_token_rahasia123 |
| Spark UI | http://localhost:8080 | - |
| Prefect | http://localhost:4200 | - |
| Trino | http://localhost:8082 | - |
| MLflow | http://localhost:5000 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| InfluxDB | http://localhost:8086 | - |

### 3. Register Prefect Flows
```bash
# Batch pipeline (setiap hari kerja jam 18:00)
prefect deployment build prefect/flows/batch_pipeline.py:batch_flow \
  --name "Batch Pipeline" --cron "0 18 * * 1-5"

# ML Training (Sen/Rab/Jum 06:00)
prefect deployment build prefect/flows/ml_training.py:ml_training_flow \
  --name "ML Training" --cron "0 6 * * 1,3,5"

# Data Quality (setiap hari kerja 07:00)
prefect deployment build prefect/flows/data_quality_flow.py:data_quality_flow \
  --name "Data Quality" --cron "0 7 * * 1-5"
```

### 4. Run Demo (Bukti Running 3x/4x/10x)
```bash
bash scripts/run_batch_demo.sh    # Batch 10x auto-run + log
bash scripts/run_all_demo.sh      # Full pipeline demo
```

### 5. Manual Run
```bash
# Batch
cd batch_processing && python scrape_historical.py

# Upload ke Garage
python batch_processing/garage_operations.py --keep-data

# Spark ETL
python batch_processing/spark_batch_etl.py

# Stream (2 terminal)
cd stream-processing && python kafka_producer.py
cd stream-processing && python spark_streaming.py

# ML
cd ml_integration && python feature_engineering.py
cd ml_integration && python train_model.py
cd ml_integration && python lstm_train.py
cd ml_integration && python lstm_predict.py

# PII Masking Demo
python dashboard_monitoring/masking_pii.py
```

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
- Alamat → generalize
- Saldo → kategorisasi (< 50jt, 50-100jt, > 100jt)
- Data fiktif disimpan di `stock-bucket/pii-sample/`

## Keamanan
- Password & credentials via environment variable (`.env`)
- Data publik YFinance tidak mengandung PII
- Audit trail semua operasi pipeline di `audit/`
- Bucket policy Garage S3 untuk access control
