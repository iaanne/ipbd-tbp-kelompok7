from prefect import flow, task
import os, sys, json, logging, boto3, pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
from botocore.config import Config

sys.path.insert(0, '/opt/prefect')
logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

def get_garage_client():
    return boto3.client('s3', endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY, aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version='s3v4'), region_name='garage')

@task
def run_data_quality():
    client = get_garage_client()
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    all_dfs = []
    for obj in sorted(objs.get('Contents', []), key=lambda x: x['LastModified']):
        data = client.get_object(Bucket=BUCKET, Key=obj['Key'])
        df = pd.read_csv(StringIO(data['Body'].read().decode('utf-8')))
        df['_source_file'] = obj['Key']
        all_dfs.append(df)
    df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    report = {'timestamp': datetime.now().isoformat(), 'total_rows': len(df), 'total_columns': len(df.columns), 'columns': {}}
    for col in df.columns:
        cr = {'dtype': str(df[col].dtype), 'null_count': int(df[col].isnull().sum()),
              'null_pct': round(float(df[col].isnull().sum() / max(len(df), 1) * 100), 2),
              'unique_count': int(df[col].nunique())}
        if df[col].dtype in ['float64', 'int64']:
            cr['min'] = float(df[col].min()) if not df[col].isnull().all() else None
            cr['max'] = float(df[col].max()) if not df[col].isnull().all() else None
            cr['mean'] = float(df[col].mean()) if not df[col].isnull().all() else None
        report['columns'][col] = cr

    report['overall'] = {'total_null': int(df.isnull().sum().sum()), 'duplicate_rows': int(df.duplicated().sum())}
    logger.info(f"DQ: {report['overall']}")

    key = f"metadata/data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(report, indent=2).encode('utf-8'))
    return report

@task
def save_metadata():
    client = get_garage_client()
    meta = {
        'table_name': 'saham_indonesia', 'description': 'Data historis saham Indonesia dari YFinance',
        'owner': 'Kelompok 7 - IPBD', 'sources': ['YFinance', 'IDX'],
        'update_frequency': 'Harian (hari kerja)', 'storage': 'Garage S3',
        'schema': [
            {'column': 'Date', 'type': 'DATE', 'description': 'Tanggal perdagangan'},
            {'column': 'Ticker', 'type': 'VARCHAR', 'description': 'Kode saham'},
            {'column': 'Nama_Saham', 'type': 'VARCHAR', 'description': 'Nama perusahaan'},
            {'column': 'Open', 'type': 'FLOAT', 'description': 'Harga pembukaan'},
            {'column': 'High', 'type': 'FLOAT', 'description': 'Harga tertinggi'},
            {'column': 'Low', 'type': 'FLOAT', 'description': 'Harga terendah'},
            {'column': 'Close', 'type': 'FLOAT', 'description': 'Harga penutupan'},
            {'column': 'Volume', 'type': 'BIGINT', 'description': 'Volume perdagangan'},
            {'column': 'Daily_Change', 'type': 'FLOAT', 'description': 'Perubahan harga harian'},
            {'column': 'Daily_Change_Pct', 'type': 'FLOAT', 'description': 'Persentase perubahan harian'},
        ],
        'pii_columns': [], 'masking_applied': 'N/A (data publik, tidak ada PII)',
        'compliance': 'Data publik Yahoo Finance',
        'created_at': datetime.now().isoformat(),
    }
    client.put_object(Bucket=BUCKET, Key='metadata/table_metadata.json', Body=json.dumps(meta, indent=2).encode('utf-8'))
    logger.info("Metadata saved")

@task
def log_audit():
    client = get_garage_client()
    entry = {'timestamp': datetime.now().isoformat(), 'action': 'data_quality_flow', 'status': 'completed', 'user': 'prefect'}
    key = f"audit/audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(entry, indent=2).encode('utf-8'))
    logger.info(f"Audit logged: {key}")

@flow(log_prints=True)
def data_quality_flow():
    run_data_quality()
    save_metadata()
    log_audit()

if __name__ == "__main__":
    data_quality_flow()
