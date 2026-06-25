from prefect import flow, task
import os, sys, json, logging, boto3, joblib
import pandas as pd
import numpy as np
import mlflow
from io import BytesIO, StringIO
from datetime import datetime
from botocore.config import Config
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score

sys.path.insert(0, '/opt/prefect')
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

@task
def load_features():
    client = get_garage_client()
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='processed-data/features/')
    feature_files = [o for o in objs.get('Contents', []) if o['Key'].endswith('.parquet')]
    if not feature_files:
        raise FileNotFoundError("No feature parquet files")

    all_dfs = []
    for ff in feature_files:
        obj = client.get_object(Bucket=BUCKET, Key=ff['Key'])
        all_dfs.append(pd.read_parquet(BytesIO(obj['Body'].read())))
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

@task
def prepare_clustering_data(df):
    cols = ['Close', 'Open', 'High', 'Low', 'Volume', 'SMA_7', 'SMA_30', 'Volatility', 'Price_Range']
    available = [c for c in cols if c in df.columns]
    data = df[available].dropna()
    logger.info(f"Clustering data: {len(data)} rows, {len(available)} features")
    return data, available

@task
def train_kmeans(X, feature_names):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    with mlflow.start_run(run_name=f"kmeans_{datetime.now().strftime('%Y%m%d_%H%M')}"):
        pipeline = Pipeline([
            ('scaler', StandardScaler()),
            ('kmeans', KMeans(n_clusters=4, random_state=42, n_init=10)),
        ])
        pipeline.fit(X)
        labels = pipeline.predict(X)
        sil = silhouette_score(X, labels)

        mlflow.log_param("n_clusters", 4)
        mlflow.log_param("n_features", len(feature_names))
        mlflow.log_param("feature_names", feature_names)
        mlflow.log_metric("inertia", pipeline.named_steps['kmeans'].inertia_)
        mlflow.log_metric("silhouette_score", sil)
        mlflow.sklearn.log_model(pipeline, "kmeans_model")

        logger.info(f"KMeans: inertia={pipeline.named_steps['kmeans'].inertia_:.2f}, sil={sil:.4f}")
        return pipeline

@task
def train_lstm(df):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    from tensorflow.keras.callbacks import EarlyStopping

    ihsg = df[df['Ticker'] == '^JKSE'].sort_values('Date')
    if len(ihsg) < 120:
        logger.warning(f"Not enough IHSG data: {len(ihsg)} rows")
        return None

    prices = ihsg['Close'].values
    seq_len = 60
    X, y = [], []
    for i in range(seq_len, len(prices)):
        X.append(prices[i-seq_len:i])
        y.append(prices[i])
    X, y = np.array(X), np.array(y)
    X = X.reshape((X.shape[0], X.shape[1], 1))

    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(seq_len, 1)),
        Dropout(0.2),
        LSTM(50, return_sequences=False),
        Dropout(0.2),
        Dense(1),
    ])
    model.compile(optimizer='adam', loss='mse')

    es = EarlyStopping(patience=10, restore_best_weights=True)
    with mlflow.start_run(run_name=f"lstm_{datetime.now().strftime('%Y%m%d_%H%M')}"):
        history = model.fit(X_train, y_train, epochs=100, batch_size=16,
            validation_data=(X_test, y_test), callbacks=[es], verbose=0)

        val_loss = min(history.history['val_loss'])
        mlflow.log_param("seq_len", seq_len)
        mlflow.log_param("epochs", len(history.history['loss']))
        mlflow.log_metric("val_loss", val_loss)
        mlflow.tensorflow.log_model(model, "lstm_model")

        logger.info(f"LSTM: val_loss={val_loss:.4f}, epochs={len(history.history['loss'])}")
    return model

@task
def save_models_to_garage(kmeans_model, lstm_model):
    client = get_garage_client()
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')

    buf = BytesIO()
    joblib.dump(kmeans_model, buf)
    buf.seek(0)
    client.put_object(Bucket=BUCKET, Key=f"models/kmeans_{date_str}.joblib", Body=buf.getvalue())

    if lstm_model:
        import tempfile, shutil
        tmp = tempfile.mkdtemp()
        lstm_model.save(f"{tmp}/lstm_ihsg.h5")
        with open(f"{tmp}/lstm_ihsg.h5", 'rb') as f:
            client.put_object(Bucket=BUCKET, Key=f"models/lstm_ihsg_{date_str}.h5", Body=f.read())
        shutil.rmtree(tmp)

    logger.info("Models saved to Garage")

@flow(log_prints=True)
def ml_training_flow():
    df = load_features()
    X, features = prepare_clustering_data(df)

    mlflow.set_experiment("stock-pipeline")
    kmeans_model = train_kmeans(X, features)
    lstm_model = train_lstm(df)
    save_models_to_garage(kmeans_model, lstm_model)

if __name__ == "__main__":
    ml_training_flow()
