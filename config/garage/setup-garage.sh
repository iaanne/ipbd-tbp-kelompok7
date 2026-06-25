#!/bin/bash

GARAGE_BIN="docker exec -i garage /garage"
ADMIN_TOKEN="admin_token_rahasia123"

echo "Menunggu Garage siap..."
sleep 3

# Konfigurasi cluster (single node)
echo "Konfigurasi layout cluster..."
$GARAGE_BIN layout assign -z dc1 -c 1G -t default $(hostname)
$GARAGE_BIN layout apply

# Buat access key untuk boto3
echo "Membuat access key..."
KEY_JSON=$($GARAGE_BIN key create --name stock-pipeline-key)
ACCESS_KEY=$(echo "$KEY_JSON" | grep -o '"AccessKeyId": "[^"]*"' | cut -d'"' -f4)
SECRET_KEY=$(echo "$KEY_JSON" | grep -o '"SecretAccessKey": "[^"]*"' | cut -d'"' -f4)

echo "Access Key ID: $ACCESS_KEY"
echo "Secret Access Key: $SECRET_KEY"

# Simpan ke file untuk referensi
echo "GARAGE_ACCESS_KEY=$ACCESS_KEY" > .garage-keys
echo "GARAGE_SECRET_KEY=$SECRET_KEY" >> .garage-keys

# Buat bucket
echo "Membuat bucket stock-bucket..."
$GARAGE_BIN bucket create stock-bucket

# Konfigurasi bucket policy
echo "Mengkonfigurasi bucket..."
$GARAGE_BIN bucket allow stock-bucket --key stock-pipeline-key --read --write --create --owner

echo "Setup Garage selesai!"
echo "Keys tersimpan di .garage-keys"
