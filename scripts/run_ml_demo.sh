#!/bin/bash
# ML PIPELINE DEMO: train KMeans 3x with different cluster counts
set -e

LOG_DIR="logs/ml_demo"
mkdir -p "$LOG_DIR"

CLUSTERS=(3 4 5)
NUM_RUNS=${#CLUSTERS[@]}

echo "=========================================="
echo "  ML PIPELINE DEMO - ${NUM_RUNS}x TRAINING"
echo "=========================================="
echo "Mulai: $(date)"
echo ""

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

for i in "${!CLUSTERS[@]}"; do
    n=${CLUSTERS[$i]}
    echo "-------------------------"
    echo "  TRAINING #$((i+1)) - KMeans with n_clusters=$n"
    echo "-------------------------"

    LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S)_${i}_k${n}.log"

    {
        echo "=== ML RUN #$((i+1)) ==="
        echo "n_clusters: $n"
        echo "Start: $(date)"
        echo ""

        echo "[STEP 1/4] Feature Engineering..."
        cd ml_integration
        python feature_engineering.py 2>&1
        cd "$BASE_DIR"
        echo ""

        echo "[STEP 2/4] Training KMeans (k=$n)..."
        cd ml_integration
        python train_model.py $n 2>&1
        cd "$BASE_DIR"
        echo ""

        echo "[STEP 3/4] Batch Inference + Enrich..."
        cd ml_integration
        python batch_inference.py 2>&1
        cd "$BASE_DIR"
        echo ""

        echo "[STEP 4/4] Simpan log execution..."
        python3 -c "
import boto3, json
from datetime import datetime
from botocore.config import Config
client = boto3.client('s3', endpoint_url='${GARAGE_ENDPOINT:-http://localhost:3900}',
    aws_access_key_id='${GARAGE_ACCESS_KEY:-GKc98624849db70446555a905b}',
    aws_secret_access_key='${GARAGE_SECRET_KEY:-934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828}',
    config=Config(signature_version='s3v4'), region_name='garage')
log = {'run': $((i+1)), 'n_clusters': $n, 'timestamp': datetime.now().isoformat(), 'status': 'completed', 'pipeline': 'ml_demo'}
client.put_object(Bucket='stock-bucket', Key='logs/ml_demo/run_${i}_k${n}.json', Body=json.dumps(log).encode('utf-8'))
print('Log saved to Garage')
" 2>&1

        echo ""
        echo "=== RUN #$((i+1)) SELESAI (k=$n) ==="
        echo "End: $(date)"
    } 2>&1 | tee "$LOG_FILE"

    echo ""
done

echo "=========================================="
echo "  ML DEMO SELESAI - ${NUM_RUNS}x TRAINING"
echo "=========================================="
echo "Logs: $LOG_DIR"
echo "Selesai: $(date)"
echo ""
echo "Model files di Garage:"
python3 -c "
import boto3
from botocore.config import Config
client = boto3.client('s3', endpoint_url='${GARAGE_ENDPOINT:-http://localhost:3900}',
    aws_access_key_id='${GARAGE_ACCESS_KEY:-GKc98624849db70446555a905b}',
    aws_secret_access_key='${GARAGE_SECRET_KEY:-934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828}',
    config=Config(signature_version='s3v4'), region_name='garage')
for prefix in ['models/', 'predictions/', 'logs/']:
    objs = client.list_objects_v2(Bucket='stock-bucket', Prefix=prefix)
    for o in sorted(objs.get('Contents', []), key=lambda x: x['Key']):
        print(f'  {o[\"Key\"]} ({o[\"Size\"]} bytes)')
"
