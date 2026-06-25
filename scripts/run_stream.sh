#!/bin/bash
# STREAM PIPELINE: Kafka Producer + Spark Streaming (real-time)
# Jalankan di terminal terpisah!
set -e

echo "============================================="
echo "  STREAM PIPELINE"
echo "  Kafka Producer → Spark Streaming"
echo "============================================="
echo ""
echo "CATATAN: Jalankan script ini di 2 terminal terpisah!"
echo ""
echo "  Terminal 1: bash scripts/run_stream.sh producer"
echo "  Terminal 2: bash scripts/run_stream.sh streaming"
echo ""

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

source .env 2>/dev/null || true

MODE="${1:-help}"

case "$MODE" in
    producer)
        echo "Menjalankan Kafka Producer (real-time stream)..."
        echo "Tekan Ctrl+C untuk berhenti"
        echo ""
        python stream-processing/kafka_producer.py
        ;;
    streaming)
        echo "Menjalankan Spark Structured Streaming..."
        echo "Tekan Ctrl+C untuk berhenti"
        echo ""
        python stream-processing/spark_streaming.py
        ;;
    realtime-ml)
        echo "Menjalankan Real-time ML Prediction..."
        echo "Tekan Ctrl+C untuk berhenti"
        echo ""
        python ml_integration/realtime_prediction.py
        ;;
    *)
        echo "Penggunaan:"
        echo "  bash scripts/run_stream.sh producer      # Terminal 1"
        echo "  bash scripts/run_stream.sh streaming     # Terminal 2"
        echo "  bash scripts/run_stream.sh realtime-ml   # Terminal 3 (opsional)"
        ;;
esac
