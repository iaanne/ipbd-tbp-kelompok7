#!/bin/bash
# =============================================
#  FULL PIPELINE: 1-Click Run
#  Setup → Batch → ML → Data Quality → Stream
# =============================================
set -e

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

source .env 2>/dev/null || true

LOG_DIR="logs/pipeline"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d_%H%M%S).log"

echo "============================================="
echo "  IPBD STOCK PIPELINE - FULL DEMO"
echo "  $(date)"
echo "============================================="
echo ""

{
    echo "============================================="
    echo "  FULL PIPELINE EXECUTION"
    echo "  Start: $(date)"
    echo "============================================="
    echo ""

    # =============================================
    # PHASE 0: SETUP
    # =============================================
    echo "============================================="
    echo "  PHASE 0: SETUP INFRASTRUKTUR"
    echo "============================================="
    echo ""

    echo "[0/5] Mengecek Docker services..."
    docker ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || {
        echo "Docker tidak jalan! Jalankan: docker-compose up -d"
        exit 1
    }
    echo ""

    echo "[0/5] Setup Garage S3..."
    bash scripts/garage-setup.sh
    echo ""

    # =============================================
    # PHASE 1: BATCH PIPELINE
    # =============================================
    echo "============================================="
    echo "  PHASE 1: BATCH PIPELINE"
    echo "============================================="
    echo ""

    echo "[1/4] Scraping data historis dari YFinance..."
    cd batch_processing
    python scrape_historical.py
    cd "$BASE_DIR"
    echo ""

    echo "[2/4] Upload CSV ke Garage S3..."
    python batch_processing/minio_operations.py --keep-data
    echo ""

    echo "[3/4] Spark Batch ETL (SMA 7/30, Volatility)..."
    python batch_processing/spark_batch_etl.py
    echo ""

    echo "[4/4] Data Quality & Metadata..."
    python dashboard_monitoring/data_quality.py
    echo ""

    # =============================================
    # PHASE 2: ML PIPELINE
    # =============================================
    echo "============================================="
    echo "  PHASE 2: MACHINE LEARNING PIPELINE"
    echo "============================================="
    echo ""

    echo "[1/5] Feature Engineering..."
    cd ml_integration
    python feature_engineering.py
    cd "$BASE_DIR"
    echo ""

    echo "[2/5] Training KMeans Clustering..."
    cd ml_integration
    python train_model.py
    cd "$BASE_DIR"
    echo ""

    echo "[3/5] Batch Inference + Enrich Prediction..."
    cd ml_integration
    python batch_inference.py
    cd "$BASE_DIR"
    echo ""

    echo "[4/5] Training LSTM Time Series..."
    cd ml_integration
    python lstm_train.py
    cd "$BASE_DIR"
    echo ""

    echo "[5/5] LSTM Prediksi IHSG + Estimasi Hari ke 6000..."
    cd ml_integration
    python lstm_predict.py
    cd "$BASE_DIR"
    echo ""

    # =============================================
    # PHASE 3: DATA GOVERNANCE
    # =============================================
    echo "============================================="
    echo "  PHASE 3: DATA GOVERNANCE & PII MASKING"
    echo "============================================="
    echo ""

    echo "[1/2] PII Masking Demo..."
    python dashboard_monitoring/masking_pii.py
    echo ""

    echo "[2/2] Verifikasi file di Garage S3..."
    python3 -c "
import boto3
from botocore.config import Config
import os

client = boto3.client('s3',
    endpoint_url=os.getenv('GARAGE_ENDPOINT', 'http://localhost:3900'),
    aws_access_key_id=os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b'),
    aws_secret_access_key=os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828'),
    config=Config(signature_version='s3v4'), region_name='garage')

bucket = os.getenv('GARAGE_BUCKET', 'stock-bucket')
prefixes = ['raw-data/', 'processed-data/', 'features/', 'models/',
            'predictions/', 'metadata/', 'audit/', 'pii-sample/']

print(f'Inventory bucket: {bucket}')
for prefix in prefixes:
    objs = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = objs.get('Contents', [])
    if files:
        print(f'  {prefix}: {len(files)} file(s)')
        for f in files[:5]:
            print(f'    {f[\"Key\"]} ({f[\"Size\"]} bytes)')
        if len(files) > 5:
            print(f'    ... dan {len(files)-5} file lainnya')
    else:
        print(f'  {prefix}: (kosong)')
"
    echo ""

    # =============================================
    # SUMMARY
    # =============================================
    echo "============================================="
    echo "  PIPELINE SELESAI"
    echo "============================================="
    echo ""
    echo "Akses Services:"
    echo "  Garage WebUI   : http://localhost:3909"
    echo "  Spark UI       : http://localhost:8080"
    echo "  Prefect UI     : http://localhost:4200"
    echo "  MLflow         : http://localhost:5000"
    echo "  Grafana        : http://localhost:3000 (admin/admin)"
    echo "  Prometheus     : http://localhost:9090"
    echo ""
    echo "Log: $LOG_FILE"

} 2>&1 | tee "$LOG_FILE"

echo ""
echo "Full pipeline selesai! Log tersimpan di: $LOG_FILE"
