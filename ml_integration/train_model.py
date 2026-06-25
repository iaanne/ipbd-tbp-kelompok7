import pandas as pd
import numpy as np
import boto3
import os
import joblib
import json
from io import BytesIO, StringIO
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'SuperSecretPassword123!')
BUCKET = 'stock-indonesia-bucket'
MODEL_PREFIX = 'models/'

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def load_latest_features(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='features/')
    feature_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('.parquet')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not feature_files:
        raise FileNotFoundError("No feature parquet files found in MinIO")

    obj = client.get_object(Bucket=BUCKET, Key=feature_files[0]['Key'])
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    logger.info(f"Loaded features: {len(df)} rows, {len(df.columns)} cols from {feature_files[0]['Key']}")
    return df

def prepare_clustering_data(df):
    cluster_features = [
        'Returns_1d', 'Returns_5d', 'Volatility_5d',
        'Volume_Change', 'Price_Range_Pct', 'Close_Open_Ratio',
        'SMA_Cross', 'RSI', 'Volume',
    ]

    available = [c for c in cluster_features if c in df.columns]
    missing = [c for c in cluster_features if c not in df.columns]
    if missing:
        logger.warning(f"Missing features for clustering: {missing}")

    data = df[available].dropna()
    logger.info(f"Clustering data: {len(data)} rows, {len(available)} features")
    return data, available

def train_clustering_model(X, feature_names, n_clusters=4):
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('kmeans', KMeans(n_clusters=n_clusters, random_state=42, n_init=10)),
    ])

    pipeline.fit(X)
    labels = pipeline.predict(X)

    inertia = pipeline.named_steps['kmeans'].inertia_
    sil_score = None
    try:
        from sklearn.metrics import silhouette_score
        sil_score = silhouette_score(X, labels)
    except:
        pass

    model_info = {
        'model_type': 'KMeans',
        'n_clusters': n_clusters,
        'n_features': len(feature_names),
        'feature_names': feature_names,
        'n_samples': len(X),
        'inertia': inertia,
        'silhouette_score': sil_score,
        'training_date': datetime.now().isoformat(),
        'cluster_distribution': pd.Series(labels).value_counts().to_dict(),
    }

    logger.info(f"Model trained: {json.dumps(model_info, indent=2)}")
    return pipeline, model_info

def save_model_to_minio(client, pipeline, model_info):
    model_buffer = BytesIO()
    joblib.dump(pipeline, model_buffer)
    model_buffer.seek(0)

    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_key = f"{MODEL_PREFIX}kmeans_cluster_{date_str}.joblib"
    info_key = f"{MODEL_PREFIX}kmeans_cluster_{date_str}_info.json"

    client.put_object(Bucket=BUCKET, Key=model_key, Body=model_buffer.getvalue())
    client.put_object(
        Bucket=BUCKET, Key=info_key,
        Body=json.dumps(model_info, indent=2).encode('utf-8')
    )

    logger.info(f"Model saved to s3://{BUCKET}/{model_key}")
    logger.info(f"Model info saved to s3://{BUCKET}/{info_key}")

    return model_key, info_key

def predict_clusters(client, pipeline, feature_names):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='features/')
    feature_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('.parquet')],
        key=lambda x: x['LastModified'], reverse=True
    )

    for ff in feature_files[:3]:
        obj = client.get_object(Bucket=BUCKET, Key=ff['Key'])
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        data = df[feature_names].dropna()

        if len(data) == 0:
            continue

        clusters = pipeline.predict(data)
        df['Cluster'] = np.nan
        df.loc[data.index, 'Cluster'] = clusters

        enrich_buffer = BytesIO()
        df.to_parquet(enrich_buffer, index=False)
        enrich_buffer.seek(0)

        enrich_key = ff['Key'].replace('features/', 'predictions/').replace('.parquet', '_enriched.parquet')
        client.put_object(Bucket=BUCKET, Key=enrich_key, Body=enrich_buffer.getvalue())
        logger.info(f"Predictions saved to s3://{BUCKET}/{enrich_key}")

        cluster_summary = df.groupby('Cluster')[['Close', 'Volume', 'Returns_1d']].mean().to_dict()
        logger.info(f"Cluster summary: {json.dumps(cluster_summary, indent=2, default=str)}")

    return True

if __name__ == '__main__':
    import sys

    client = get_minio_client()
    df = load_latest_features(client)

    X, feature_names = prepare_clustering_data(df)
    model, model_info = train_clustering_model(X, feature_names, n_clusters=4)

    model_key, info_key = save_model_to_minio(client, model, model_info)
    predict_clusters(client, model, feature_names)

    print(f"Training selesai. Model: {model_key}")
    print(f"Info: {info_key}")
