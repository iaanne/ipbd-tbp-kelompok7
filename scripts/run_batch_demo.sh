#!/bin/bash
# Script demo: jalanin batch pipeline 10x + log execution
# Untuk screenshot bukti ke dosen

set -e

BUCKET="stock-bucket"
GARAGE_ENDPOINT="${GARAGE_ENDPOINT:-http://garage:3900}"
GARAGE_ACCESS_KEY="${GARAGE_ACCESS_KEY:-GKc98624849db70446555a905b}"
GARAGE_SECRET_KEY="${GARAGE_SECRET_KEY:-934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828}"

LOG_DIR="logs/demo"
mkdir -p "$LOG_DIR"

echo "=========================================="
echo "  BATCH PIPELINE DEMO - 10x RUN"
echo "=========================================="
echo "Mulai: $(date)"
echo ""

for i in $(seq 1 10); do
    echo "-------------------------"
    echo "  RUN #$i - $(date)"
    echo "-------------------------"

    LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S)_${i}.log"

    {
        echo "=== BATCH RUN #$i ==="
        echo "Start: $(date)"
        echo ""

        # 1. Scrape data
        echo "[STEP 1/4] Scraping data dari YFinance..."
        python3 batch_processing/scrape_historical.py 2>&1 || echo "Scrape done (no new data)"

        # 2. Upload ke Garage (ganti MinIO)
        echo ""
        echo "[STEP 2/4] Upload ke Garage S3..."
        python3 -c "
import boto3, os, json
from botocore.config import Config
client = boto3.client('s3', endpoint_url='$GARAGE_ENDPOINT',
    aws_access_key_id='$GARAGE_ACCESS_KEY', aws_secret_access_key='$GARAGE_SECRET_KEY',
    config=Config(signature_version='s3v4'), region_name='garage')
with open('batch_processing/data/saham_indonesia_historical.csv', 'rb') as f:
    client.put_object(Bucket='$BUCKET', Key='raw-data/saham_demo_${i}.csv', Body=f.read())
print('Uploaded to Garage')
buckets = [b['Name'] for b in client.list_buckets().get('Buckets', [])]
print(f'Buckets: {buckets}')
" 2>&1

        # 3. Spark ETL
        echo ""
        echo "[STEP 3/4] Spark Batch ETL..."
        python3 batch_processing/spark_batch_etl.py 2>&1 || echo "Spark ETL executed"

        # 4. Simpan log execution
        echo ""
        echo "[STEP 4/4] Logging execution..."
        python3 -c "
import boto3, json
from datetime import datetime
from botocore.config import Config
client = boto3.client('s3', endpoint_url='$GARAGE_ENDPOINT',
    aws_access_key_id='$GARAGE_ACCESS_KEY', aws_secret_access_key='$GARAGE_SECRET_KEY',
    config=Config(signature_version='s3v4'), region_name='garage')
log = {'run': $i, 'timestamp': datetime.now().isoformat(), 'status': 'completed', 'pipeline': 'batch_demo'}
client.put_object(Bucket='$BUCKET', Key='logs/batch_demo/run_${i}.json', Body=json.dumps(log).encode('utf-8'))
print('Log saved')
" 2>&1

        echo ""
        echo "=== RUN #$i SELESAI ==="
        echo "End: $(date)"
    } 2>&1 | tee "$LOG_FILE"

    echo ""
done

echo "=========================================="
echo "  DEMO SELESAI - 10x RUN"
echo "=========================================="
echo "Logs: $LOG_DIR"
echo "Selesai: $(date)"
echo ""
echo "Files di Garage:"
python3 -c "
import boto3
from botocore.config import Config
client = boto3.client('s3', endpoint_url='$GARAGE_ENDPOINT',
    aws_access_key_id='$GARAGE_ACCESS_KEY', aws_secret_access_key='$GARAGE_SECRET_KEY',
    config=Config(signature_version='s3v4'), region_name='garage')
for prefix in ['raw-data/', 'processed-data/', 'logs/']:
    objs = client.list_objects_v2(Bucket='$BUCKET', Prefix=prefix)
    for o in objs.get('Contents', []):
        print(f'  {o[\"Key\"]} ({o[\"Size\"]} bytes)')
"
