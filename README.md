# ipbd-stock-pipeline-kelompok7

Project IPBD (Infrastruktur dan Platform Big Data) – Real-time Ticker & Prediksi Pergerakan Harga Saham Indonesia

Repo ini merupakan monorepo yang menggabungkan **Batch Processing**, **Stream Processing**, **Machine Learning Integration**, **Dashboard & Monitoring**, dan **MinIO Object Storage** untuk pipeline big data harga saham index lokal Indonesia (IDX).

## 📁 Struktur Folder
├── 1-batch-processing/       
│   ├── scrape_historical.py      
│   ├── minio_operations.py   
│   ├── spark_batch_etl.py    
│   └── data/                        
│
|
├── 2-stream-processing/      
│   ├── kafka_producer.py       
│   ├── spark_streaming.py     
│   └── checkpoints/                
│
|
├── 3-ml-integration/        
│   ├── feature_engineering.py  
│   ├── train_model.py          
│   ├── batch_inference.py       
│   └── realtime_prediction.py  
│
|
├── 4-dashboard-monitoring/   
│   ├── grafana-dashboards/
│   │   └── stock-dashboard.json    
│   ├── prometheus/
│   │   └── prometheus.yml           
│   ├── alertmanager/
│   │   └── alertmanager.yml         
│   └── docker-compose.override.yml  
│
|
├── 5-docs/                   
│   ├── screenshots/
│   │   ├── ss_1_minio_login.png
│   │   ├── ss_2_minio_dashboard.png
│   │   ├── ss_3_create_bucket.png
│   │   ├── ss_4_upload_csv.png
│   │   └── ss_5_delete_bucket.png
│   └── laporan/
│       └── template_laporan.md      
│
|
├── 6-config/               
│   ├── .env.example                 
│   └── minio-policy.json            
│
├── docker-compose.yml        
├── requirements.txt
├── .gitignore               
└── README.md                 


## 👥 Anggota Kelompok & Pembagian Tugas

| No | Nama | NIM | 
|----|------|-----|
| 1 | Adrian Farrel Aziz Yatyoga | L0224040 |
| 2 | Michael Christian Shan Geraldo | L0224035 | 

# Pembagian Detail
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
git clone git@github.com:iaanne/ipbd-tbp-kelompok7.git
cd ipbd-stock-pipeline-kelompok7

# Copy environment config
cp config/.env.example .env

# Install Python dependencies
pip install -r requirements.txt

# Jalankan semua service: MinIO, Kafka, Spark, Grafana, Prometheus, InfluxDB
docker-compose up -d

# Cek status container
docker-compose ps

cd batch-processing

# Step 1: Scraping data historis dari Yahoo Finance
python scrape_historical.py

# Step 2: Upload CSV ke MinIO (screenshot untuk PDF!)
python minio_operations.py

# Step 3: Spark Batch ETL (baca dari MinIO → transform → save ke MinIO)
python spark_batch_etl.py


cd stream-processing

# Terminal 1: Jalankan Kafka Producer (simulasi data real-time)
python kafka_producer.py

# Terminal 2: Jalankan Spark Streaming (baca dari Kafka)
python spark_streaming.py


cd ml-integration

# Step 1: Feature Engineering (baca processed data dari MinIO)
python feature_engineering.py

# Step 2: Training Model
python train_model.py

# Step 3: Batch Inference (prediksi data historis)
python batch_inference.py

# Step 4: Real-time Prediction API (FastAPI)
python realtime_prediction.py


# Import dashboard template ke Grafana
# 1. Buka http://localhost:3000
# 2. Login: admin / admin
# 3. Dashboards → Import → Upload 4-dashboard-monitoring/grafana-dashboards/stock-dashboard.json

# Alert rules sudah di-config di prometheus.yml dan alertmanager.yml

