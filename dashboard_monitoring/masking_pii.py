import os, json, hashlib, boto3, pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

def get_garage_client():
    return boto3.client('s3', endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY, aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version='s3v4'), region_name='garage')

def create_sample_pii_data():
    data = [
        {'user_id': 1, 'nama_lengkap': 'Budi Santoso', 'email': 'budi.santoso@gmail.com', 'no_telepon': '081234567890', 'alamat': 'Jl. Merdeka No. 1, Jakarta', 'saldo_investasi': 50000000, 'risk_profile': 'moderate'},
        {'user_id': 2, 'nama_lengkap': 'Siti Rahmawati', 'email': 'siti.rahma@yahoo.com', 'no_telepon': '082345678901', 'alamat': 'Jl. Sudirman No. 45, Bandung', 'saldo_investasi': 100000000, 'risk_profile': 'aggressive'},
        {'user_id': 3, 'nama_lengkap': 'Ahmad Hidayat', 'email': 'ahmad.hidayat@outlook.com', 'no_telepon': '083456789012', 'alamat': 'Jl. Diponegoro No. 78, Surabaya', 'saldo_investasi': 25000000, 'risk_profile': 'conservative'},
        {'user_id': 4, 'nama_lengkap': 'Dewi Lestari', 'email': 'dewi.lestari@gmail.com', 'no_telepon': '084567890123', 'alamat': 'Jl. Gatot Subroto No. 12, Yogyakarta', 'saldo_investasi': 75000000, 'risk_profile': 'moderate'},
        {'user_id': 5, 'nama_lengkap': 'Rudi Hermawan', 'email': 'rudi.hermawan@company.co.id', 'no_telepon': '085678901234', 'alamat': 'Jl. Thamrin No. 33, Medan', 'saldo_investasi': 150000000, 'risk_profile': 'aggressive'},
    ]
    return pd.DataFrame(data)

def mask_email(email):
    local, domain = email.split('@')
    masked_local = local[0] + '****' + local[-1] if len(local) > 2 else local[0] + '****'
    return f"{masked_local}@{domain}"

def mask_nama(nama):
    parts = nama.split()
    return parts[0] + ' ' + parts[-1][0] + '***' if len(parts) > 1 else nama

def mask_telepon(no):
    return no[:3] + '****' + no[-3:]

def mask_alamat(alamat):
    parts = alamat.split(',')
    jalan = parts[0].strip()
    jalan_parts = jalan.split()
    if len(jalan_parts) >= 2:
        masked_jalan = jalan_parts[0] + ' ***** ' + jalan_parts[-1]
    else:
        masked_jalan = jalan
    return masked_jalan + ', ' + ', '.join(parts[1:]) if len(parts) > 1 else masked_jalan

def generalize_saldo(saldo):
    if saldo >= 100000000:
        return "> 100 juta"
    elif saldo >= 50000000:
        return "50-100 juta"
    else:
        return "< 50 juta"

def main():
    client = get_garage_client()
    df = create_sample_pii_data()

    df_masked = df.copy()
    df_masked['email'] = df['email'].apply(mask_email)
    df_masked['nama_lengkap'] = df['nama_lengkap'].apply(mask_nama)
    df_masked['no_telepon'] = df['no_telepon'].apply(mask_telepon)
    df_masked['alamat'] = df['alamat'].apply(mask_alamat)
    df_masked['saldo_investasi_kategori'] = df['saldo_investasi'].apply(generalize_saldo)
    df_masked = df_masked.drop(columns=['saldo_investasi'])

    buf_orig = StringIO()
    df.to_csv(buf_orig, index=False)
    buf_masked = StringIO()
    df_masked.to_csv(buf_masked, index=False)

    client.put_object(Bucket=BUCKET, Key='pii-sample/data_trader_original.csv', Body=buf_orig.getvalue().encode('utf-8'))
    client.put_object(Bucket=BUCKET, Key='pii-sample/data_trader_masked.csv', Body=buf_masked.getvalue().encode('utf-8'))

    hash_doc = {
        'timestamp': datetime.now().isoformat(),
        'method': 'SHA-256 hashing + masking',
        'fields_masked': ['email (partial mask)', 'nama (partial mask)', 'no_telepon (partial mask)', 'alamat (generalize)', 'saldo_investasi (kategorisasi)'],
        'original_file': 'pii-sample/data_trader_original.csv',
        'masked_file': 'pii-sample/data_trader_masked.csv',
        'note': 'Data fiktif untuk demo masking PII. Data asli saham dari YFinance tidak mengandung PII.',
    }
    client.put_object(Bucket=BUCKET, Key='pii-sample/masking_documentation.json', Body=json.dumps(hash_doc, indent=2).encode('utf-8'))

    logger.info("=== PII Masking Demo ===")
    logger.info(f"Original: {len(df)} rows -> saved to pii-sample/data_trader_original.csv")
    logger.info(f"Masked: {len(df_masked)} rows -> saved to pii-sample/data_trader_masked.csv")
    logger.info(f"\nContoh Original vs Masked:")
    for i in range(2):
        logger.info(f"\n  User {df.iloc[i]['user_id']}:")
        logger.info(f"    Nama:     {df.iloc[i]['nama_lengkap']:30s} -> {df_masked.iloc[i]['nama_lengkap']}")
        logger.info(f"    Email:    {df.iloc[i]['email']:30s} -> {df_masked.iloc[i]['email']}")
        logger.info(f"    Telepon:  {df.iloc[i]['no_telepon']:30s} -> {df_masked.iloc[i]['no_telepon']}")
        logger.info(f"    Alamat:   {df.iloc[i]['alamat']:30s} -> {df_masked.iloc[i]['alamat']}")
        logger.info(f"    Saldo:    Rp {df.iloc[i]['saldo_investasi']:,} -> {df_masked.iloc[i]['saldo_investasi_kategori']}")

if __name__ == '__main__':
    main()
