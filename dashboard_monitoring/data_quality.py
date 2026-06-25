import pandas as pd
import numpy as np
import boto3
import os
import json
from io import StringIO, BytesIO
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://localhost:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

def get_garage_client():
    return boto3.client(
        's3',
        endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
    )

def load_all_raw_data(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    all_dfs = []
    for obj in sorted(objs.get('Contents', []), key=lambda x: x['LastModified']):
        data = client.get_object(Bucket=BUCKET, Key=obj['Key'])
        df = pd.read_csv(StringIO(data['Body'].read().decode('utf-8')))
        df['_source_file'] = obj['Key']
        all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def check_data_quality(df):
    report = {
        'timestamp': datetime.now().isoformat(),
        'dataset': 'saham_indonesia',
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': {},
    }

    for col in df.columns:
        col_report = {
            'dtype': str(df[col].dtype),
            'total': int(df[col].count()),
            'null_count': int(df[col].isnull().sum()),
            'null_pct': round(float(df[col].isnull().sum() / max(len(df), 1) * 100), 2),
            'unique_count': int(df[col].nunique()),
        }

        if df[col].dtype in ['float64', 'int64']:
            col_report['min'] = float(df[col].min()) if not df[col].isnull().all() else None
            col_report['max'] = float(df[col].max()) if not df[col].isnull().all() else None
            col_report['mean'] = float(df[col].mean()) if not df[col].isnull().all() else None
            col_report['std'] = float(df[col].std()) if not df[col].isnull().all() else None

            expected_numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'Daily_Change', 'Daily_Change_Pct']
            if col in expected_numeric_cols:
                non_numeric = df[col].apply(lambda x: not isinstance(x, (int, float))).sum()
                col_report['type_errors'] = int(non_numeric)
                col_report['type_error_pct'] = round(float(non_numeric / max(len(df), 1) * 100), 2)

        expected_date_cols = ['Date']
        if col in expected_date_cols:
            try:
                pd.to_datetime(df[col])
                col_report['date_parse_errors'] = 0
            except:
                col_report['date_parse_errors'] = int(df[col].isnull().sum())

        report['columns'][col] = col_report

    report['overall'] = {
        'total_null': int(df.isnull().sum().sum()),
        'rows_with_null': int(df.isnull().any(axis=1).sum()),
        'rows_with_null_pct': round(float(df.isnull().any(axis=1).sum() / max(len(df), 1) * 100), 2),
        'duplicate_rows': int(df.duplicated().sum()),
        'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
    }

    return report

def save_quality_report(client, report):
    key = f"metadata/data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(report, indent=2, default=str).encode('utf-8')
    )
    logger.info(f"Data quality report saved to s3://{BUCKET}/{key}")
    return key

def get_table_metadata():
    return {
        'table_name': 'saham_indonesia',
        'description': 'Data historis harga saham Indonesia dari YFinance',
        'owner': 'Kelompok 7 - IPBD',
        'sources': ['YFinance', 'IDX'],
        'update_frequency': 'Harian (hari kerja)',
        'storage': 'Garage S3 (S3-compatible)',
        'schema': [
            {'column': 'Date', 'type': 'DATE', 'description': 'Tanggal perdagangan'},
            {'column': 'Ticker', 'type': 'VARCHAR', 'description': 'Kode saham (contoh: BBCA, ^JKSE)'},
            {'column': 'Nama_Saham', 'type': 'VARCHAR', 'description': 'Nama perusahaan'},
            {'column': 'Open', 'type': 'FLOAT', 'description': 'Harga pembukaan'},
            {'column': 'High', 'type': 'FLOAT', 'description': 'Harga tertinggi'},
            {'column': 'Low', 'type': 'FLOAT', 'description': 'Harga terendah'},
            {'column': 'Close', 'type': 'FLOAT', 'description': 'Harga penutupan'},
            {'column': 'Volume', 'type': 'BIGINT', 'description': 'Volume perdagangan'},
            {'column': 'Daily_Change', 'type': 'FLOAT', 'description': 'Perubahan harga harian (Close - Open)'},
            {'column': 'Daily_Change_Pct', 'type': 'FLOAT', 'description': 'Persentase perubahan harian'},
        ],
        'pii_columns': [],
        'masking_applied': 'N/A (tidak ada data PII dalam data saham)',
        'compliance': 'Data publik dari Yahoo Finance. Tidak ada data pribadi.',
        'created_at': datetime.now().isoformat(),
        'version': '1.0.0',
    }

def save_metadata(client, metadata):
    key = "metadata/table_metadata.json"
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(metadata, indent=2, default=str).encode('utf-8')
    )
    logger.info(f"Metadata saved to s3://{BUCKET}/{key}")

def log_audit_trail(client, action, status, details=None):
    audit_entry = {
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'status': status,
        'user': 'prefect_kelompok7',
        'details': details or {},
    }
    key = f"audit/audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(audit_entry, indent=2).encode('utf-8')
    )
    logger.info(f"Audit trail logged: {action} -> {status}")

if __name__ == '__main__':
    client = get_garage_client()

    df = load_all_raw_data(client)
    logger.info(f"Loaded {len(df)} rows for data quality check")

    report = check_data_quality(df)
    save_quality_report(client, report)

    metadata = get_table_metadata()
    save_metadata(client, metadata)

    log_audit_trail(client, 'data_quality_check', 'completed', {
        'rows_checked': len(df),
        'quality_score': f"{100 - report['overall']['rows_with_null_pct']:.1f}%"
    })

    print("\n=== DATA QUALITY REPORT ===")
    print(f"Total Rows: {report['total_rows']}")
    print(f"Rows with Null: {report['overall']['rows_with_null']} ({report['overall']['rows_with_null_pct']}%)")
    print(f"Duplicate Rows: {report['overall']['duplicate_rows']}")

    for col, col_report in report['columns'].items():
        if col_report['null_count'] > 0:
            print(f"  - {col}: {col_report['null_count']} nulls ({col_report['null_pct']}%)")
        if 'type_errors' in col_report and col_report['type_errors'] > 0:
            print(f"  - {col}: {col_report['type_errors']} type errors ({col_report['type_error_pct']}%)")

    print("\n=== TABLE METADATA ===")
    print(f"Owner: {metadata['owner']}")
    print(f"Description: {metadata['description']}")
    print(f"Columns: {len(metadata['schema'])}")
    print(f"PII: {'None' if not metadata['pii_columns'] else metadata['pii_columns']}")
