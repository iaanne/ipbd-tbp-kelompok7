#!/bin/bash
# BATCH PIPELINE: Scrape → Garage → Spark ETL
set -e

echo "============================================="
echo "  BATCH PIPELINE"
echo "  Scrape → Garage S3 → Spark ETL"
echo "============================================="
echo ""

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

source .env 2>/dev/null || true

LOG_DIR="logs/batch"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/batch_$(date +%Y%m%d_%H%M%S).log"

{
    echo "=== BATCH PIPELINE ==="
    echo "Start: $(date)"
    echo ""

    # STEP 1: Scrape
    echo "[STEP 1/3] Scraping data historis dari YFinance..."
    cd batch_processing
    python scrape_historical.py
    cd "$BASE_DIR"
    echo ""

    # STEP 2: Upload ke Garage S3
    echo "[STEP 2/3] Upload ke Garage S3..."
    python batch_processing/minio_operations.py --keep-data
    echo ""

    # STEP 3: Spark Batch ETL
    echo "[STEP 3/3] Spark Batch ETL (SMA, Volatility)..."
    python batch_processing/spark_batch_etl.py
    echo ""

    echo "=== BATCH PIPELINE SELESAI ==="
    echo "End: $(date)"
} 2>&1 | tee "$LOG_FILE"

echo ""
echo "Log: $LOG_FILE"
