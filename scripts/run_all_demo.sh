#!/bin/bash
# Script demo: jalanin semua pipeline + screenshot untuk laporan
set -e

echo "=========================================="
echo "  IPBD STOCK PIPELINE - FULL DEMO"
echo "=========================================="
echo ""

# 1. Cek service
echo ">>> [1] Cek service containers..."
docker ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || echo "Docker not running"

# 2. Batch demo
echo ""
echo ">>> [2] Batch Pipeline - 10x Run"
bash scripts/run_batch_demo.sh

# 3. Data Quality & Metadata
echo ""
echo ">>> [3] Data Quality & Metadata"
python3 -c "
from dashboard_monitoring.data_quality import *
client = get_garage_client()
df = load_all_raw_data(client)
report = check_data_quality(df)
save_quality_report(client, report)
metadata = get_table_metadata()
save_metadata(client, metadata)
log_audit_trail(client, 'full_demo', 'completed')
print('Data Quality & Metadata done')
" 2>&1 || echo "DQ script not fully adapted yet, running standalone..."
python3 dashboard_monitoring/data_quality.py 2>&1 | tail -5

# 4. PII Masking Demo
echo ""
echo ">>> [4] PII Masking Demo"
python3 dashboard_monitoring/masking_pii.py 2>&1

# 5. ML Training (KMeans + LSTM)
echo ""
echo ">>> [5] ML Training Pipeline"
python3 ml_integration/feature_engineering.py 2>&1 | tail -5
python3 ml_integration/train_model.py 2>&1 | tail -10
python3 ml_integration/lstm_train.py 2>&1 | tail -10

# 6. Batch Inference + Prediksi
echo ""
echo ">>> [6] Batch Inference + Prediction"
python3 ml_integration/batch_inference.py 2>&1 | tail -10
python3 ml_integration/lstm_predict.py 2>&1 | tail -10

# 7. List semua data di Garage
echo ""
echo ">>> [7] Garage S3 - File Inventory"
python3 -c "
import boto3
from botocore.config import Config
GARAGE_ENDPOINT = '${GARAGE_ENDPOINT:-http://garage:3900}'
GARAGE_ACCESS_KEY = '${GARAGE_ACCESS_KEY:-GKc98624849db70446555a905b}'
GARAGE_SECRET_KEY = '${GARAGE_SECRET_KEY:-934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828}'
BUCKET = '${GARAGE_BUCKET:-stock-bucket}'
client = boto3.client('s3', endpoint_url=GARAGE_ENDPOINT,
    aws_access_key_id=GARAGE_ACCESS_KEY, aws_secret_access_key=GARAGE_SECRET_KEY,
    config=Config(signature_version='s3v4'), region_name='garage')

prefixes = ['raw-data/', 'processed-data/', 'features/', 'models/',
            'predictions/', 'metadata/', 'audit/', 'logs/', 'pii-sample/']
for prefix in prefixes:
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    files = objs.get('Contents', [])
    if files:
        print(f'{prefix}: {len(files)} files')
        for f in files[:3]:
            print(f'  {f[\"Key\"]} ({f[\"Size\"]} bytes)')
        if len(files) > 3:
            print(f'  ... and {len(files)-3} more')
    else:
        print(f'{prefix}: (empty)')
"

echo ""
echo "=========================================="
echo "  FULL DEMO COMPLETED"
echo "=========================================="
echo "Screenshot untuk laporan:"
echo "  1. Garage WebUI (http://localhost:3909) - isi bucket"
echo "  2. Prefect UI (http://localhost:4200) - flow runs"
echo "  3. MLflow UI (http://localhost:5000) - experiment tracking"
echo "  4. Grafana (http://localhost:3000) - dashboard"
echo "  5. Log file di logs/demo/"
