# Laporan Proyek IPBD - Real-time Ticker & Prediksi Pergerakan Harga Saham

## Tim
| Nama | NIM | Peran |
|------|-----|-------|
| Adrian Farrel Aziz Yatyoga | L0224040 | Data Engineer & Infrastructure |
| Michael Christian Shan Geraldo | L0224035 | ML Engineer & Analytics |

## 1. Arsitektur Pipeline
Lihat [ARCHITECTURE.md](./ARCHITECTURE.md) untuk diagram arsitektur lengkap dengan Mermaid.

**Komponen utama:**
- **Data Source**: YFinance & IDX
- **Ingestion**: Batch scraper + Kafka producer
- **Storage**: Garage S3 (S3-compatible object storage)
- **Processing**: Apache Spark (batch & streaming)
- **Query**: Trino + Hive Metastore
- **ML Lifecycle**: MLflow (tracking, experiment, model registry)
- **Orchestration**: Prefect (flows + agent)
- **Monitoring**: Prometheus + InfluxDB + Grafana
- **Notification**: Email + Telegram via Alertmanager

## 2. Batch Processing
- **Source**: YFinance (6 tickers: ^JKSE, BBCA.JK, BBRI.JK, BMRI.JK, TLKM.JK, ASII.JK)
- **Storage**: Garage S3 bucket `stock-bucket/raw-data/`
- **Proses**: Scrape data → Upload to Garage → Spark ETL (SMA, Volatility, Price Range)
- **Frekuensi**: Harian (hari kerja, dijadwalkan Prefect)
- **Bukti running 3x/4x/10x**: Demo script `scripts/run_batch_demo.sh` — otomatis jalan 10x + log execution + screenshot

## 3. Stream Processing
- **Source**: YFinance real-time via Kafka producer
- **Broker**: Kafka topic `stock-stream-topic`
- **Proses**: Kafka Producer (60s interval) → Spark Structured Streaming (5-min window)
- **Frekuensi**: Real-time
- **Bukti**: Data tersimpan di Garage (stream-data/) + log execution

## 4. Machine Learning (Training & Prediction)

### KMeans Clustering
- **Input**: 18 fitur teknis (Returns, SMA, Volatility, RSI, Price Range, dll.)
- **Model**: 4 cluster (Low/Medium-Low/Medium-High/High risk)
- **Training**: 3x per minggu (Senin, Rabu, Jumat via Prefect)
- **Output**: Cluster label → enrich data asli → prediksi risiko

### LSTM Time Series
- **Input**: Sequence 60 hari harga Close IHSG
- **Arsitektur**: LSTM(50)→Dropout(0.2)→LSTM(50)→Dropout(0.2)→Dense(25)→Dense(1)
- **Loss**: MSE, Optimizer: Adam, EarlyStopping patience=10
- **Output**: Prediksi harga IHSG 7 hari ke depan + estimasi hari ke 6000
- **Training**: 1x per minggu (Minggu via Prefect)

### MLflow Tracking
- Semua experiment tercatat (params, metrics, model artifacts)
- Model disimpan di Garage via MLflow artifact store
- Bisa bandingkan runs: `mlflow.search_runs(order_by=['metrics.val_loss ASC'])`

## 5. Dashboard Visualisasi

### Stock Analysis Dashboard (Grafana)
- **Tujuan**: Melihat pergerakan IHSG, memprediksi kapan menyentuh 6000
- **Panel**: IHSG + SMA trend, Volume, RSI, Cluster distribution, LSTM prediction, hari ke 6000
- **Insight**: Investor bisa lihat apakah market sehat (volatilitas rendah, cluster low-risk dominan)

### Pipeline Monitoring Dashboard (Grafana)
- **Tujuan**: Memantau pipeline health
- **Panel**: Flow status, log severity (INFO/DEBUG/WARNING/FATAL), Spark duration, data freshness, quality score

## 6. Monitoring Pipeline
- **Grafana**: Dashboard monitoring (port 3000)
- **Prometheus**: Metrics + alert rules (port 9090)
- **InfluxDB**: Time-series pipeline log (port 8086)
- **Alertmanager**: Trigger notifikasi (port 9093)
- **Log severity**: INFO, DEBUG, WARNING, FATAL — tercatat di InfluxDB & ditampilkan di Grafana

## 7. Notifikasi Pipeline Failure
- **Prefect**: `on_failure` callback → trigger flow `alert_notify_flow`
- **Email**: SMTP Gmail ke `team@example.com`
- **Telegram**: Bot API ke chat group
- **Alertmanager**: Juga mengirim alert via webhook

## 8. Keamanan & Konfigurasi
- **Environment Variable**: Semua password (Garage, SMTP, Telegram, InfluxDB) via `.env`
- **Masking PII**: Script `masking_pii.py` mendemonstrasikan:
  - Email → partial mask (`b****o@gmail.com`)
  - Nama → partial mask (`Budi S***`)
  - Telepon → partial mask (`0812****890`)
  - Alamat → generalize
  - Saldo → kategorisasi (`< 50 juta`, `50-100 juta`, `> 100 juta`)
  - Data asli & masked disimpan di bucket `pii-sample/`
- **Garage credentials** via env variable, tidak hardcoded

## 9. Data Quality, Metadata, Audit Trail
- **Data Quality** (`data_quality.py`):
  - Null values per kolom + persentase
  - Type errors (kolom numerik berisi non-numeric)
  - Duplicate rows
  - Date parse errors
- **Metadata**: Setiap tabel punya dokumentasi:
  - Owner (Kelompok 7)
  - Deskripsi, schema kolom, source data, update frequency
- **Audit Trail**: Semua operasi pipeline dicatat di `audit/` (timestamp, action, status, user)
- **Compliance**: Data publik Yahoo Finance, tidak ada data pribadi

## 10. Cara Menjalankan

```bash
# 1. Setup
cp .env.example .env
docker-compose up -d

# 2. Akses services
# Garage WebUI: http://localhost:3909
# Spark UI:     http://localhost:8080
# Trino:        http://localhost:8082
# MLflow:       http://localhost:5000
# Prefect:      http://localhost:4200
# Grafana:      http://localhost:3000 (admin/admin)
# Prometheus:   http://localhost:9090
# InfluxDB:     http://localhost:8086

# 3. Register Prefect flows
prefect deployment build prefect/flows/batch_pipeline.py:batch_flow \
  --name "Batch Pipeline" --cron "0 18 * * 1-5"
prefect deployment build prefect/flows/ml_training.py:ml_training_flow \
  --name "ML Training" --cron "0 6 * * 1,3,5"
prefect deployment build prefect/flows/data_quality_flow.py:data_quality_flow \
  --name "Data Quality" --cron "0 7 * * 1-5"

# 4. Demo 10x batch
bash scripts/run_batch_demo.sh

# 5. Full demo
bash scripts/run_all_demo.sh
```

## Screenshots
Letakkan di `docs/screenshots/`:
1. `ss_1_garage_webui.png` - Garage WebUI dengan isi bucket
2. `ss_2_prefect_flows.png` - Prefect UI flow runs
3. `ss_3_mlflow_tracking.png` - MLflow experiment tracking
4. `ss_4_spark_ui.png` - Spark job execution
5. `ss_5_kafka_messages.png` - Kafka topic messages
6. `ss_6_grafana_stock.png` - Stock Analysis dashboard
7. `ss_7_grafana_monitoring.png` - Pipeline Monitoring dashboard
8. `ss_8_batch_demo_logs.png` - Log execution 10x batch
9. `ss_9_pii_masking.png` - PII masking demo (original vs masked)
10. `ss_10_trino_query.png` - Trino SQL query
