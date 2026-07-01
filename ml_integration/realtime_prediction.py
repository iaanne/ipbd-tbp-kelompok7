import json
import os
import sys
import joblib
import numpy as np
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

from botocore.config import Config

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
        config=Config(signature_version='s3v4'),
        region_name='garage'
    )

def load_model_from_garage(client, model_key=None):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='models/')
    model_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('.joblib')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not model_files:
        raise FileNotFoundError("No trained model found")

    key = model_key or model_files[0]['Key']
    obj = client.get_object(Bucket=BUCKET, Key=key)
    model = joblib.load(BytesIO(obj['Body'].read()))
    return model, key

def load_model_info(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='models/')
    info_files = sorted(
        [o for o in objs.get('Contents', []) if o['Key'].endswith('_info.json')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not info_files:
        return None
    obj = client.get_object(Bucket=BUCKET, Key=info_files[0]['Key'])
    return json.loads(obj['Body'].read().decode('utf-8'))

def extract_features_from_stream(ticker, row):
    features = {
        'Returns_1d': 0.0,
        'Returns_5d': 0.0,
        'Volatility_5d': 0.0,
        'Volume_Change': 0.0,
        'Price_Range_Pct': (row.get('high', 0) - row.get('low', 0)) / max(row.get('open', 1), 0.01),
        'Close_Open_Ratio': row.get('close', 0) / max(row.get('open', 1), 0.01),
        'SMA_Cross': 0.0,
        'RSI': 50.0,
        'Volume': row.get('volume', 0),
    }
    return features

def predict_stock_cluster(model, features, feature_names):
    df = pd.DataFrame([features])[feature_names]
    cluster = int(model.predict(df)[0])
    return cluster

def enrich_prediction(ticker, row, cluster, model_info):
    enriched = {
        'timestamp': datetime.now().isoformat(),
        'ticker': ticker,
        'open': row.get('open'),
        'high': row.get('high'),
        'low': row.get('low'),
        'close': row.get('close'),
        'volume': row.get('volume'),
        'cluster': cluster,
        'risk_level': 'high' if cluster >= 3 else 'medium' if cluster >= 2 else 'low',
        'market_sentiment': 'bullish' if cluster <= 1 else 'bearish',
        'prediction_time': datetime.now().isoformat(),
    }
    return enriched

def save_prediction_to_garage(client, enriched):
    date_str = datetime.now().strftime('%Y%m%d')
    key = f"predictions/realtime/{date_str}/{enriched['ticker']}_{datetime.now().strftime('%H%M%S')}.json"
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(enriched, indent=2).encode('utf-8')
    )
    return key

if __name__ == '__main__':
    client = get_garage_client()

    model_info = load_model_info(client)
    feature_names = model_info['feature_names'] if model_info else []
    model, model_key = load_model_from_garage(client)

    logger.info(f"Model loaded: {model_key}")
    logger.info(f"Feature names: {feature_names}")

    from kafka import KafkaConsumer, KafkaProducer
    import json as json_mod

    consumer = KafkaConsumer(
        'stock-stream-topic',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda v: json_mod.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
    )

    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json_mod.dumps(v).encode('utf-8'),
    )

    logger.info("Listening for real-time stock data...")

    for msg in consumer:
        data = msg.value
        ticker = data.get('ticker', 'unknown')

        features = extract_features_from_stream(ticker, data)
        cluster = predict_stock_cluster(model, features, feature_names)
        enriched = enrich_prediction(ticker, data, cluster, model_info)

        file_key = save_prediction_to_garage(client, enriched)
        logger.info(f"{ticker} -> Cluster {cluster} (Risk: {enriched['risk_level']})")

        enriched['type'] = 'prediction'
        kafka_producer.send('stock-prediction-topic', enriched)
