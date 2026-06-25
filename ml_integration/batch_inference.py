import pandas as pd
import numpy as np
import boto3
import os
import joblib
import json
from io import BytesIO, StringIO
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'SuperSecretPassword123!')
BUCKET = 'stock-indonesia-bucket'

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def load_latest_model(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='models/')
    model_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('.joblib')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not model_files:
        raise FileNotFoundError("No model files found in MinIO")

    obj = client.get_object(Bucket=BUCKET, Key=model_files[0]['Key'])
    model = joblib.load(BytesIO(obj['Body'].read()))
    logger.info(f"Loaded model from {model_files[0]['Key']}")
    return model, model_files[0]['Key']

def load_latest_model_info(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='models/')
    info_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('_info.json')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not info_files:
        return None
    obj = client.get_object(Bucket=BUCKET, Key=info_files[0]['Key'])
    return json.loads(obj['Body'].read().decode('utf-8'))

def load_new_data_for_inference(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='features/')
    feature_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('.parquet')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not feature_files:
        raise FileNotFoundError("No feature files found")

    all_dfs = []
    for ff in feature_files[:5]:
        obj = client.get_object(Bucket=BUCKET, Key=ff['Key'])
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['Ticker', 'Date'])

def enrich_with_predictions(df, model, feature_names):
    data = df[feature_names].dropna()
    if len(data) == 0:
        logger.warning("No valid data for inference")
        return df

    clusters = model.predict(data)
    df['Cluster'] = np.nan
    df.loc[data.index, 'Cluster'] = clusters

    cluster_means = df[df['Cluster'].notna()].groupby('Cluster')['Close'].mean()
    df['Cluster_Avg_Close'] = df['Cluster'].map(cluster_means)
    df['Price_vs_Cluster'] = df['Close'] - df['Cluster_Avg_Close']

    cluster_vol = df[df['Cluster'].notna()].groupby('Cluster')['Volatility_5d'].mean()
    df['Cluster_Avg_Volatility'] = df['Cluster'].map(cluster_vol)

    for cluster_id in sorted(df['Cluster'].dropna().unique()):
        cid = int(cluster_id)
        cluster_data = df[df['Cluster'] == cid]
        total = len(cluster_data)
        up_count = (cluster_data['Returns_1d'] > 0).sum()
        pct_up = (up_count / total * 100) if total > 0 else 0
        logger.info(f"Cluster {cid}: {total} samples, {pct_up:.1f}% bullish")

    return df

def save_enriched_data(client, df):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"predictions/batch_enriched_{date_str}.parquet"
    client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    logger.info(f"Enriched predictions saved to s3://{BUCKET}/{key}")
    return key

def analyze_ihsg_prediction(df):
    ihsg = df[df['Ticker'] == '^JKSE'].copy()
    if ihsg.empty:
        return {"message": "No IHSG data available"}

    latest = ihsg.iloc[-1]
    clusters_in_ihsg = ihsg['Cluster'].value_counts().to_dict() if 'Cluster' in ihsg.columns else {}
    recent_trend = ihsg['Close'].tail(5).tolist()

    analysis = {
        'latest_close': float(latest['Close']),
        'latest_cluster': int(latest['Cluster']) if 'Cluster' in ihsg.columns and pd.notna(latest['Cluster']) else None,
        'ihsg_cluster_distribution': {str(k): int(v) for k, v in clusters_in_ihsg.items()},
        'recent_close_trend': [float(x) for x in recent_trend],
        'days_to_6000': None,
    }

    if analysis['latest_close'] is not None:
        gap = 6000 - analysis['latest_close']
        avg_daily_change = ihsg['Returns_1d'].mean()
        if avg_daily_change and avg_daily_change > 0 and gap > 0:
            days = gap / (analysis['latest_close'] * avg_daily_change)
            analysis['days_to_6000'] = max(1, int(abs(days)))

    logger.info(f"IHSG Analysis: {json.dumps(analysis, indent=2)}")
    return analysis

if __name__ == '__main__':
    client = get_minio_client()

    model_info = load_latest_model_info(client)
    logger.info(f"Model info: {model_info}")

    model, model_path = load_latest_model(client)
    feature_names = model_info['feature_names'] if model_info else []

    df = load_new_data_for_inference(client)
    logger.info(f"Loaded {len(df)} rows for inference")

    df_enriched = enrich_with_predictions(df, model, feature_names)
    save_path = save_enriched_data(client, df_enriched)

    analysis = analyze_ihsg_prediction(df_enriched)
    print(f"\n=== IHSG Analysis ===")
    print(f"Latest Close: {analysis['latest_close']}")
    print(f"Days to 6000: {analysis['days_to_6000']} days (estimated)")
    print(f"\nEnriched data saved to: {save_path}")
