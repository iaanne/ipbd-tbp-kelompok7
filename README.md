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
│   └── data/                  # Output CSV lokal hasil scraping
├── stream-processing/         # Kafka Producer + Spark Structured Streaming
├── ml_integration/            # Feature Eng, KMeans, LSTM, Prediksi
│   └── models/                # Saved model files (.joblib, .h5)
├── prefect/                   # Orchestration flows (Prefect)
│   ├── Dockerfile             # Custom Prefect agent image
│   ├── requirements.txt       # Prefect agent dependencies
│   └── flows/                 # Flow definitions (batch, ML, DQ, monitoring, alert)
├── dashboard_monitoring/      # Streamlit, Grafana, Prometheus, InfluxDB, Alertmanager
│   ├── grafana-provisioning/  # Auto-provisioning datasources & dashboards
│   ├── grafana-dashboards/    # Pre-built JSON dashboards (stock + pipeline)
│   ├── prometheus/            # Prometheus config + alert rules
│   └── alertmanager/          # Alertmanager config (email, Telegram, WhatsApp)
├── config/                    # Konfigurasi service
│   ├── garage/                # Garage S3 config + setup script
│   ├── hive/                  # Hive Metastore config
│   ├── trino/                 # Trino config + catalog files
│   └── postgresql/            # HMS init SQL
├── scripts/                   # Pipeline scripts (.sh)
├── docs/                      # Dokumentasi arsitektur & laporan
│   └── screenshots/           # Bukti eksekusi pipeline
├── data/                      # Dataset historis saham
├── logs/                      # Log eksekusi pipeline
│   └── demo/                  # 10x batch demo run logs
├── docker-compose.yml         # Semua service container (14+ services)
├── .env.example               # Contoh konfigurasi environment
├── requirements.txt           # Python dependencies
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
  Data Quality  →  PII Masking  →  Grafana Dashboard (auto-provisioned)  →  Alerting (Email, Telegram, WhatsApp)
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

### 5. Demo Batch 10x Run (Bukti ke Dosen)
```bash
# Batch pipeline jalan 10x otomatis + log
bash scripts/run_batch_demo.sh
```

### 6. Demo Stream 3x/4x/10x
```bash
# Stream pipeline jalan N kali (default 3)
bash scripts/run_stream_demo.sh 3
bash scripts/run_stream_demo.sh 10
```

### 7. Demo ML Training 3x
```bash
# Training KMeans dengan k=3, k=4, k=5
bash scripts/run_ml_demo.sh
```

### 8. Full Pipeline Demo
```bash
# Batch → DQ → PII → ML → Inventory
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

# 6. Setup mc alias untuk verifikasi (isi ACCESS/SECRET dari .env)
mc alias set garage http://localhost:3900 $GARAGE_ACCESS_KEY $GARAGE_SECRET_KEY
mc ls --recursive garage/stock-bucket/
```

### Phase 1: Batch Pipeline
```bash
# Step 1: Scrape data historis dari YFinance (6 saham)
cd batch_processing
python scrape_historical.py
# Output: data/saham_indonesia_historical.csv

# Step 2: Upload CSV ke Garage S3 (buat bucket + upload)
python minio_operations.py --keep-data
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/raw-data/

# Step 3: Spark Batch ETL (hitung SMA 7/30, Volatility, Price Range)
python spark_batch_etl.py
# Output: s3://stock-bucket/processed-data/features/
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/processed-data/

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
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/features/

# Step 2: Training KMeans Clustering (4 cluster risiko)
python train_model.py
# Output: Model + predictions ke s3://stock-bucket/models/ & predictions/
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/models/ && mc ls --recursive garage/stock-bucket/predictions/

# Step 3: Batch Inference + Enrich Data
python batch_inference.py
# Output: Enriched predictions + IHSG analysis (estimasi hari ke 6000)
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/predictions/ | grep batch

# Step 4: Training LSTM Time Series
python lstm_train.py
# Output: Model .h5 ke s3://stock-bucket/models/ + MLflow tracking
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/models/ | grep lstm

# Step 5: LSTM Prediksi + Estimasi Hari ke 6000
python lstm_predict.py
# Output: Prediksi 7 hari ke depan + estimasi kapan IHSG tembus 6000
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/predictions/lstm/
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
# 1. Data Quality Check (nulls, duplicates, type errors, metadata)
cd dashboard_monitoring
python data_quality.py
# Output: Quality report + metadata + audit trail ke s3://stock-bucket/metadata/ & audit/
# ✅ Verifikasi: mc ls --recursive garage/stock-bucket/metadata/ && mc ls --recursive garage/stock-bucket/audit/

# 2. PII Masking Demo (Email, Nama, Telepon, Alamat, Saldo)
python masking_pii.py
# Output: Original + masked CSV ke s3://stock-bucket/pii-sample/
```

### Monitoring
Setelah semua service Docker berjalan, akses:
- **Grafana** di http://localhost:3000 (admin/admin) — dashboard auto-provisioned untuk analisis saham & monitoring pipeline
- **Prometheus** di http://localhost:9090 — metrics pipeline execution
- **InfluxDB** di http://localhost:8086 — time-series storage untuk metrics
- **Alertmanager** — notifikasi gagal pipeline via **Email**, **Telegram**, dan **WhatsApp** (CallMeBot API)

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

# Stream Monitoring (setiap 30 menit)
prefect deployment build prefect/flows/stream_monitor.py:stream_monitor_flow \
  --name "Stream Monitor" --interval 1800

# Push Metrics to InfluxDB (setiap 15 menit)
prefect deployment build prefect/flows/push_metrics.py:push_metrics_flow \
  --name "Push Metrics" --interval 900

# Alert Notification (Email/Telegram/WhatsApp fallback)
prefect deployment build prefect/flows/alert_notify.py:alert_notify_flow \
  --name "Alert Notify"
```

---

## Access Services
| Service | URL | Login |
|---------|-----|-------|
| Garage WebUI | http://localhost:3909 | admin_token_rahasia123 |
| Spark UI (Master) | http://localhost:8080 | - |
| Spark UI (Worker) | http://localhost:8081 | - |
| Prefect UI | http://localhost:4200 | - |
| Trino SQL | http://localhost:8082 | - |
| MLflow | http://localhost:5000 | - |
| Kafka UI | http://localhost:9000 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| InfluxDB | http://localhost:8086 | - |
| Streamlit Dashboard | http://localhost:8501 | - |

## Verifikasi Cepat (mc ls)

Setup alias Garage (cukup sekali):
```bash
mc alias set garage http://localhost:3900 GK390299d18f1adea498888b9e e27d368e62e42c3ff53d2a844ab236cf87cf8ce3d7de88fe186e5932db0c135a
```

Cek isi bucket:
```bash
mc ls --recursive garage/stock-bucket/
```

Berdasarkan prefix:
```bash
mc ls --recursive garage/stock-bucket/raw-data/          # CSV mentah
mc ls --recursive garage/stock-bucket/processed-data/    # Hasil Spark ETL
mc ls --recursive garage/stock-bucket/features/         # Feature engineering
mc ls --recursive garage/stock-bucket/models/           # Model ML (.joblib, .h5)
mc ls --recursive garage/stock-bucket/predictions/      # Enriched + LSTM predictions
mc ls --recursive garage/stock-bucket/metadata/         # Data quality report
mc ls --recursive garage/stock-bucket/stream-data/      # Hasil Spark Streaming
mc ls --recursive garage/stock-bucket/pii-sample/       # PII masking demo
```

## Streamlit Dashboard
Dashboard visualisasi interaktif untuk monitoring pipeline dan analisis saham.

### Cara Menjalankan
```bash
# Aktifkan virtual environment (kalau pake venv)
source venv/bin/activate

# Atau install langsung
pip install -r requirements.txt

# Jalankan Streamlit
streamlit run dashboard_monitoring/streamlit_app.py
# → Buka http://localhost:8501
```

### Halaman Dashboard
| Halaman | Fitur |
|---------|-------|
| 📈 IHSG Overview | Chart Close + SMA 7/30, Volume, Volatilitas, metric cards |
| 📊 Stock Analysis | Perbandingan harga antar saham, bar chart volume, filter saham |
| 🤖 ML Predictions | Pie chart cluster KMeans, scatter plot Return vs Volatility, LSTM prediction 7 hari, estimasi IHSG 6000 |
| 📋 Data Quality | Quality report, metadata schema, audit trail log |
| 🔐 PII Masking | Side-by-side original vs masked data |
| 📦 File Inventory | List semua file di Garage S3 per prefix |

## Tech Stack
- **Data Source**: YFinance, IDX
- **Storage**: Garage S3 (S3-compatible, Rust-based)
- **Streaming**: Apache Kafka, Spark Structured Streaming
- **Batch**: Apache Spark (Hadoop S3A)
- **Query Engine**: Trino + Hive Metastore
- **ML Lifecycle**: MLflow (Tracking, Model Registry, Artifacts)
- **ML Models**: KMeans Clustering (risk profiling) + LSTM (price prediction)
- **Orchestration**: Prefect (Flows + Agent)
- **Monitoring**: Prometheus, InfluxDB 2.x, Grafana (auto-provisioned dashboards)
- **Notification**: Email (SMTP), Telegram (Bot API), WhatsApp (CallMeBot API) — fallback chain

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
