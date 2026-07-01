#!/bin/bash
# STREAM PIPELINE DEMO: run 3x/4x/10x with logs
set -e

BUCKET="stock-bucket"
GARAGE_ENDPOINT="${GARAGE_ENDPOINT:-http://garage:3900}"
GARAGE_ACCESS_KEY="${GARAGE_ACCESS_KEY:-GKc98624849db70446555a905b}"
GARAGE_SECRET_KEY="${GARAGE_SECRET_KEY:-934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828}"

NUM_RUNS="${1:-3}"
LOG_DIR="logs/stream_demo"
mkdir -p "$LOG_DIR"

echo "=========================================="
echo "  STREAM PIPELINE DEMO - ${NUM_RUNS}x RUN"
echo "=========================================="
echo "Mulai: $(date)"
echo ""

for i in $(seq 1 $NUM_RUNS); do
    echo "-------------------------"
    echo "  RUN #$i - $(date)"
    echo "-------------------------"

    LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S)_${i}.log"

    {
        echo "=== STREAM RUN #$i ==="
        echo "Start: $(date)"
        echo ""

        echo "[STEP 1/3] Kafka Producer (30 detik)..."
        timeout 30 python3 stream-processing/kafka_producer.py 2>&1 || echo "Producer done"

        echo ""
        echo "[STEP 2/3] Verifikasi Kafka topic..."
        python3 -c "
from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('stock-stream-topic', bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', consumer_timeout_ms=5000)
msgs = [msg for msg in consumer]
print(f'Messages in topic: {len(msgs)}')
consumer.close()
" 2>&1

        echo ""
        echo "[STEP 3/3] Simpan log execution..."
        python3 -c "
import boto3, json
from datetime import datetime
from botocore.config import Config
client = boto3.client('s3', endpoint_url='$GARAGE_ENDPOINT',
    aws_access_key_id='$GARAGE_ACCESS_KEY', aws_secret_access_key='$GARAGE_SECRET_KEY',
    config=Config(signature_version='s3v4'), region_name='garage')
log = {'run': $i, 'timestamp': datetime.now().isoformat(), 'status': 'completed', 'pipeline': 'stream_demo'}
client.put_object(Bucket='$BUCKET', Key='logs/stream_demo/run_${i}.json', Body=json.dumps(log).encode('utf-8'))
print('Log saved to Garage')
" 2>&1

        echo ""
        echo "=== RUN #$i SELESAI ==="
        echo "End: $(date)"
    } 2>&1 | tee "$LOG_FILE"

    echo ""
done

echo "=========================================="
echo "  DEMO SELESAI - ${NUM_RUNS}x RUN"
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
for prefix in ['stream-data/', 'logs/']:
    objs = client.list_objects_v2(Bucket='$BUCKET', Prefix=prefix)
    for o in objs.get('Contents', []):
        print(f'  {o[\"Key\"]} ({o[\"Size\"]} bytes)')
"
