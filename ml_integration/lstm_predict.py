import os, sys, json, boto3
import numpy as np
import pandas as pd
import tensorflow as tf
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

def get_garage_client():
    return boto3.client('s3', endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY, aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version='s3v4'), region_name='garage')

def load_latest_model(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='models/')
    model_files = sorted(
        [o for o in objs.get('Contents', []) if 'lstm' in o['Key'] and o['Key'].endswith('.h5')],
        key=lambda x: x['LastModified'], reverse=True
    )
    if not model_files:
        raise FileNotFoundError("No LSTM model found")

    obj = client.get_object(Bucket=BUCKET, Key=model_files[0]['Key'])
    tmp = "/tmp/lstm_model.h5"
    with open(tmp, 'wb') as f:
        f.write(obj['Body'].read())
    model = tf.keras.models.load_model(tmp)
    logger.info(f"Loaded model: {model_files[0]['Key']}")
    return model

def load_recent_data(client, days=90):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    all_dfs = []
    for obj in sorted(objs.get('Contents', []), key=lambda x: x['LastModified']):
        data = client.get_object(Bucket=BUCKET, Key=obj['Key'])
        all_dfs.append(pd.read_csv(StringIO(data['Body'].read().decode('utf-8'))))
    df = pd.concat(all_dfs, ignore_index=True)
    ihsg = df[df['Ticker'] == '^JKSE'].sort_values('Date')
    logger.info(f"Loaded {len(ihsg)} IHSG rows")
    return ihsg

def predict_future(model, last_60_days, days_ahead=7):
    model.eval  # no-op for tf
    predictions = []
    current_seq = last_60_days.copy()

    for _ in range(days_ahead):
        X = current_seq[-60:].reshape(1, 60, 1)
        pred = model.predict(X, verbose=0)[0][0]
        predictions.append(pred)
        current_seq = np.append(current_seq, pred)

    return predictions

def main():
    client = get_garage_client()
    model = load_latest_model(client)
    ihsg = load_recent_data(client)

    prices = ihsg['Close'].values.astype(float)
    if len(prices) < 60:
        raise ValueError(f"Need at least 60 days, got {len(prices)}")

    last_60 = prices[-60:]
    predictions = predict_future(model, last_60, days_ahead=7)

    # Hitung hari ke 6000
    ihsg_last_date = pd.to_datetime(ihsg['Date'].iloc[-1])
    pred_dates = [(ihsg_last_date + timedelta(days=i+1)).strftime('%Y-%m-%d') for i in range(len(predictions))]

    days_to_6000 = None
    for i, p in enumerate(predictions):
        if p >= 6000:
            if i == 0:
                days_to_6000 = "Hari ini sudah di atas 6000"
            else:
                days_to_6000 = i + 1
            break

    result = {
        'prediction_date': datetime.now().isoformat(),
        'latest_close': float(prices[-1]),
        'predictions': {pred_dates[i]: float(round(predictions[i], 2)) for i in range(len(predictions))},
        'days_to_6000': days_to_6000,
        'model': 'LSTM(50)->Dropout->LSTM(50)->Dropout->Dense(25)->Dense(1)',
        'seq_len': 60,
    }

    logger.info(f"Latest IHSG: {prices[-1]:.0f}")
    for d, p in zip(pred_dates, predictions):
        logger.info(f"  {d}: {p:.0f}")
    logger.info(f"Days to 6000: {days_to_6000}")

    # Simpan prediksi ke Garage
    key = f"predictions/lstm/lstm_prediction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(result, indent=2).encode('utf-8'))
    logger.info(f"Predictions saved to {BUCKET}/{key}")

    with open('/tmp/lstm_prediction.json', 'w') as f:
        json.dump(result, f, indent=2)

    return result

if __name__ == '__main__':
    main()
