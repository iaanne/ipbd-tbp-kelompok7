#!/bin/bash
# ML PIPELINE: Feature Engineering → KMeans → LSTM → Prediksi
set -e

echo "============================================="
echo "  ML PIPELINE"
echo "  Feature Eng → KMeans → LSTM → Prediksi IHSG"
echo "============================================="
echo ""

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

source .env 2>/dev/null || true

LOG_DIR="logs/ml"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/ml_$(date +%Y%m%d_%H%M%S).log"

{
    echo "=== ML PIPELINE ==="
    echo "Start: $(date)"
    echo ""

    echo "[STEP 1/5] Feature Engineering (18 fitur teknis)..."
    cd ml_integration
    python feature_engineering.py
    cd "$BASE_DIR"
    echo ""

    echo "[STEP 2/5] Training KMeans Clustering..."
    cd ml_integration
    python train_model.py
    cd "$BASE_DIR"
    echo ""

    echo "[STEP 3/5] Batch Inference + Enrich..."
    cd ml_integration
    python batch_inference.py
    cd "$BASE_DIR"
    echo ""

    echo "[STEP 4/5] Training LSTM Time Series..."
    cd ml_integration
    python lstm_train.py
    cd "$BASE_DIR"
    echo ""

    echo "[STEP 5/5] LSTM Prediksi + Estimasi Hari ke 6000..."
    cd ml_integration
    python lstm_predict.py
    cd "$BASE_DIR"
    echo ""

    echo "=== ML PIPELINE SELESAI ==="
    echo "End: $(date)"
} 2>&1 | tee "$LOG_FILE"

echo ""
echo "Log: $LOG_FILE"
