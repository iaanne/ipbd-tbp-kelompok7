import pandas as pd
import numpy as np
import boto3
import os
from io import StringIO, BytesIO
from datetime import datetime, timedelta
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

def load_historical_data(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    all_dfs = []
    for obj in sorted(objs.get('Contents', []), key=lambda x: x['LastModified']):
        data = client.get_object(Bucket=BUCKET, Key=obj['Key'])
        df = pd.read_csv(StringIO(data['Body'].read().decode('utf-8')))
        all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def engineer_features(df):
    required_cols = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

    features = []
    for ticker, group in df.groupby('Ticker'):
        g = group.copy()
        g['Returns_1d'] = g['Close'].pct_change()
        g['Returns_5d'] = g['Close'].pct_change(5)
        g['SMA_5'] = g['Close'].rolling(5).mean()
        g['SMA_20'] = g['Close'].rolling(20).mean()
        g['Volatility_5d'] = g['Returns_1d'].rolling(5).std()
        g['Volume_Change'] = g['Volume'].pct_change()
        g['Price_Range'] = g['High'] - g['Low']
        g['Price_Range_Pct'] = g['Price_Range'] / g['Open'].replace(0, np.nan)
        g['Close_Open_Ratio'] = g['Close'] / g['Open'].replace(0, np.nan)
        g['High_Low_Ratio'] = g['High'] / g['Low'].replace(0, np.nan)
        g['Lag_1_Close'] = g['Close'].shift(1)
        g['Lag_2_Close'] = g['Close'].shift(2)
        g['Lag_3_Close'] = g['Close'].shift(3)
        g['SMA_Cross'] = g['SMA_5'] - g['SMA_20']
        g['RSI'] = compute_rsi(g['Close'], 14)
        g['Target_Up'] = (g['Close'].shift(-1) > g['Close']).astype(int)
        g['Target_Change'] = g['Close'].shift(-1) - g['Close']

        features.append(g)

    result = pd.concat(features, ignore_index=True)
    result = result.dropna(subset=['Returns_1d', 'Volatility_5d'])
    result = result.replace([np.inf, -np.inf], np.nan).dropna(subset=['Returns_1d', 'Volatility_5d'])

    return result

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.replace([np.inf, -np.inf], 50)

def upload_features(client, df):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"features/engineered_features_{date_str}.parquet"
    client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    logger.info(f"Features uploaded to s3://{BUCKET}/{key}")
    return f"s3://{BUCKET}/{key}"

if __name__ == '__main__':
    client = get_garage_client()
    raw_df = load_historical_data(client)
    logger.info(f"Loaded {len(raw_df)} rows of historical data")

    feature_df = engineer_features(raw_df)
    logger.info(f"Engineered {len(feature_df.columns)} features for {len(feature_df)} rows")

    path = upload_features(client, feature_df)
    print(f"Features saved at: {path}")
    print(f"Feature columns: {list(feature_df.columns)}")
