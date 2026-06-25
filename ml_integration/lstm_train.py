import os, sys, json, boto3
import numpy as np
import pandas as pd
import mlflow
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from io import BytesIO, StringIO
from datetime import datetime
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')

def get_garage_client():
    return boto3.client('s3', endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY, aws_secret_access_key=GARAGE_SECRET_KEY,
        config=Config(signature_version='s3v4'), region_name='garage')

def load_ihsg_data(client):
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    all_dfs = []
    for obj in sorted(objs.get('Contents', []), key=lambda x: x['LastModified']):
        data = client.get_object(Bucket=BUCKET, Key=obj['Key'])
        all_dfs.append(pd.read_csv(StringIO(data['Body'].read().decode('utf-8'))))
    df = pd.concat(all_dfs, ignore_index=True)
    ihsg = df[df['Ticker'] == '^JKSE'].sort_values('Date')
    logger.info(f"Loaded {len(ihsg)} IHSG rows")
    return ihsg

def prepare_sequences(prices, seq_len=60):
    X, y = [], []
    for i in range(seq_len, len(prices)):
        X.append(prices[i-seq_len:i])
        y.append(prices[i])
    X = np.array(X).reshape(-1, seq_len, 1)
    y = np.array(y)
    split = int(len(X) * 0.8)
    return X[:split], X[split:], y[:split], y[split:]

def build_lstm(seq_len=60):
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(seq_len, 1)),
        Dropout(0.2),
        LSTM(50, return_sequences=False),
        Dropout(0.2),
        Dense(25),
        Dense(1),
    ])
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    return model

def train():
    client = get_garage_client()
    ihsg = load_ihsg_data(client)
    prices = ihsg['Close'].values.astype(float)

    X_train, X_test, y_train, y_test = prepare_sequences(prices)
    logger.info(f"Train: {len(X_train)}, Test: {len(X_test)}")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("stock-pipeline")

    with mlflow.start_run(run_name=f"lstm_ihsg_{datetime.now().strftime('%Y%m%d_%H%M')}"):
        model = build_lstm()
        mlflow.log_param("seq_len", 60)
        mlflow.log_param("layers", "LSTM(50)->Dropout->LSTM(50)->Dropout->Dense(25)->Dense(1)")
        mlflow.log_param("optimizer", "adam")
        mlflow.log_param("loss", "mse")

        es = EarlyStopping(patience=10, restore_best_weights=True)
        history = model.fit(X_train, y_train, epochs=100, batch_size=16,
            validation_data=(X_test, y_test), callbacks=[es], verbose=1)

        val_loss = min(history.history['val_loss'])
        val_mae = min(history.history['val_mae'])
        mlflow.log_metric("val_loss", val_loss)
        mlflow.log_metric("val_mae", val_mae)
        mlflow.log_metric("epochs_completed", len(history.history['loss']))
        mlflow.tensorflow.log_model(model, "lstm_ihsg_model")

        logger.info(f"LSTM trained: val_loss={val_loss:.4f}, val_mae={val_mae:.4f}")

        # Buat prediksi sample
        y_pred = model.predict(X_test[-10:])
        for i in range(10):
            logger.info(f"  Actual: {y_test[-10:][i]:.0f} | Predicted: {y_pred[i][0]:.0f}")

        # Simpan ke Garage
        tmp = "/tmp/lstm_ihsg.h5"
        model.save(tmp)
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(tmp, 'rb') as f:
            client.put_object(Bucket=BUCKET, Key=f"models/lstm_ihsg_{date_str}.h5", Body=f.read())
        logger.info(f"Model saved to Garage: models/lstm_ihsg_{date_str}.h5")
        os.remove(tmp)

        return model

if __name__ == '__main__':
    train()
