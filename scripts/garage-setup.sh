#!/bin/bash
# Setup Garage S3: cluster init + bucket + key
set -e

GARAGE_BIN="docker exec -i garage /garage"
ADMIN_TOKEN="admin_token_rahasia123"

echo "============================================="
echo "  SETUP GARAGE S3"
echo "============================================="

echo "[1/4] Menunggu Garage siap..."
sleep 3

echo "[2/4] Konfigurasi layout cluster..."
$GARAGE_BIN layout assign -z dc1 -c 1G -t default $(hostname) 2>/dev/null || true
$GARAGE_BIN layout apply 2>/dev/null || true

echo "[3/4] Membuat access key..."
$GARAGE_BIN key create --name stock-pipeline-key 2>/dev/null || true

echo "[4/4] Membuat bucket stock-bucket..."
$GARAGE_BIN bucket create stock-bucket 2>/dev/null || true
$GARAGE_BIN bucket allow stock-bucket --key stock-pipeline-key --read --write --create --owner 2>/dev/null || true

echo ""
echo "Garage S3 siap digunakan!"
echo "  Bucket: stock-bucket"
echo "  Endpoint: http://localhost:3900"
echo "  WebUI: http://localhost:3909"
echo ""
