import boto3
from botocore.client import Config
import pandas as pd
from datetime import datetime
import os

# konfigurasi GARAGE S3 (pengganti MinIO)
GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://localhost:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET_NAME = os.getenv('GARAGE_BUCKET', 'stock-bucket')

# inisialisasi client S3 (Garage-compatible)
s3_client = boto3.client(
    "s3",
    endpoint_url=GARAGE_ENDPOINT,
    aws_access_key_id=GARAGE_ACCESS_KEY,
    aws_secret_access_key=GARAGE_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="garage"
)

# function
def create_bucket(bucket_name):
    """Membuat bucket di MinIO"""
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f" Bucket '{bucket_name}' berhasil dibuat!")
        return True
    except s3_client.exceptions.BucketAlreadyExists:
        print(f"  Bucket '{bucket_name}' sudah ada")
        return True
    except Exception as e:
        print(f" Error membuat bucket: {e}")
        return False

def upload_csv_to_minio(local_csv_path, bucket_name, object_path):
    """Upload file CSV ke MinIO"""
    try:
        if not os.path.exists(local_csv_path):
            print(f" File tidak ditemukan: {local_csv_path}")
            return False
        
        s3_client.upload_file(local_csv_path, bucket_name, object_path)
        print(f" CSV '{local_csv_path}' → '{bucket_name}/{object_path}'")
        return True
    except Exception as e:
        print(f" Error upload: {e}")
        return False

def upload_dataframe_to_minio(df, bucket_name, object_path):
    """Upload DataFrame langsung sebagai CSV ke MinIO"""
    try:
        csv_buffer = df.to_csv(index=False).encode('utf-8')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_path,
            Body=csv_buffer,
            ContentType='text/csv'
        )
        print(f" DataFrame diupload ke '{bucket_name}/{object_path}'")
        return True
    except Exception as e:
        print(f" Error upload DataFrame: {e}")
        return False

def list_objects(bucket_name, prefix=""):
    """List semua object dalam bucket"""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        
        if not objects:
            print(f" Bucket '{bucket_name}/{prefix}' kosong")
            return []
        
        print(f"\n Object dalam '{bucket_name}/{prefix}':")
        for obj in objects:
            size_kb = obj['Size'] / 1024
            print(f"    {obj['Key']} ({size_kb:.2f} KB)")
        return objects
    except Exception as e:
        print(f" Error list objects: {e}")
        return []

def download_file(bucket_name, object_path, local_path):
    """Download file dari MinIO"""
    try:
        s3_client.download_file(bucket_name, object_path, local_path)
        print(f" Downloaded: '{bucket_name}/{object_path}' → '{local_path}'")
        return True
    except Exception as e:
        print(f" Error download: {e}")
        return False

def delete_object(bucket_name, object_path):
    """Hapus object dari bucket"""
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_path)
        print(f" Object '{object_path}' dihapus")
        return True
    except Exception as e:
        print(f" Error delete object: {e}")
        return False

def delete_bucket(bucket_name):
    """Hapus bucket dan semua isinya"""
    try:
        # hapus semua object dulu
        objects = list_objects(bucket_name)
        for obj in objects:
            delete_object(bucket_name, obj['Key'])
        
        # hapus bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f" Bucket '{bucket_name}' dihapus!")
        return True
    except Exception as e:
        print(f" Error delete bucket: {e}")
        return False

# main exec
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--keep-data', action='store_true', default=True, help='Jangan hapus bucket setelah selesai')
    parser.add_argument('--cleanup', action='store_true', help='Hapus bucket setelah demo')
    args = parser.parse_args()

    print("=" * 70)
    print("GARAGE S3 OPERATIONS - BATCH PROCESSING")
    print("Upload CSV Saham Indonesia ke Object Storage")
    print("=" * 70)
    
    CSV_FILE = "data/saham_indonesia_historical.csv"
    
    if not os.path.exists(CSV_FILE):
        print(f"\n File CSV belum ada!")
        print(f"   Jalankan dulu: scrape_historical.py")
        exit(1)
    
    df = pd.read_csv(CSV_FILE)
    print(f"\n File CSV ditemukan:")
    print(f"   Path: {CSV_FILE}")
    print(f"   Baris: {len(df)}")
    print(f"   Kolom: {list(df.columns)}")
    print(f"   Saham: {df['Ticker'].unique()}")
    
    print("\n" + "-" * 70)
    print("STEP 1: CREATE BUCKET")
    print("-" * 70)
    print(f"   Nama bucket: {BUCKET_NAME}")
    create_bucket(BUCKET_NAME)
    
    print("\n" + "-" * 70)
    print("STEP 2: UPLOAD CSV KE BUCKET")
    print("-" * 70)
    upload_csv_to_minio(CSV_FILE, BUCKET_NAME, "raw-data/saham_indonesia_historical.csv")
    for ticker in df['Ticker'].unique():
        df_ticker = df[df['Ticker'] == ticker]
        upload_dataframe_to_minio(df_ticker, BUCKET_NAME, f"per-ticker/{ticker}_daily.csv")
    
    print("\n" + "-" * 70)
    print("STEP 3: LIST OBJECTS (Verifikasi)")
    print("-" * 70)
    list_objects(BUCKET_NAME)
    list_objects(BUCKET_NAME, "raw-data/")
    list_objects(BUCKET_NAME, "per-ticker/")
  
    print("\n" + "-" * 70)
    print("STEP 4: DOWNLOAD DEMO")
    print("-" * 70)
    download_file(BUCKET_NAME, "raw-data/saham_indonesia_historical.csv", "downloaded_saham_indonesia.csv")
    
    if args.cleanup:
        print("\n" + "-" * 70)
        print("STEP 5: DELETE BUCKET")
        print("-" * 70)
        delete_bucket(BUCKET_NAME)
    else:
        print("\n" + "-" * 70)
        print("STEP 5: SKIP DELETE (--keep-data)")
        print("-" * 70)
        print("   Bucket & data tetap disimpan untuk pipeline berikutnya.")
    
    print("\n" + "=" * 70)
    print("GARAGE S3 OPERATIONS SELESAI!")
    print("=" * 70)
    print("   1. Login page Garage WebUI (localhost:3909)")
    print("   2. Dashboard setelah login")
    print("   3. Bucket 'stock-bucket' berisi:")
    print("      - raw-data/saham_indonesia_historical.csv")
    print("      - per-ticker/{ticker}_daily.csv")
