from prefect import flow, task
from datetime import datetime, timedelta
import os, sys, json, logging, boto3
import pandas as pd
from io import StringIO
from botocore.config import Config
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from alert_notify import alert_notify_flow

sys.path.insert(0, '/opt/prefect')

logger = logging.getLogger(__name__)

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.environ['GARAGE_ACCESS_KEY']
GARAGE_SECRET_KEY = os.environ['GARAGE_SECRET_KEY']
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

@task(retries=2, retry_delay_seconds=30)
def check_environment():
    required = ['GARAGE_ENDPOINT', 'GARAGE_ACCESS_KEY', 'GARAGE_SECRET_KEY']
    missing = [e for e in required if not os.getenv(e)]
    if missing:
        raise ValueError(f"Missing env vars: {missing}")
    logger.info("Environment OK")
    return "OK"

@task(retries=2, retry_delay_seconds=30)
def scrape_stock_data():
    import yfinance as yf
    tickers = {
        '^JKSE': 'IHSG', 'BBCA.JK': 'Bank Central Asia',
        'BBRI.JK': 'Bank Rakyat Indonesia', 'BMRI.JK': 'Bank Mandiri',
        'TLKM.JK': 'Telkom Indonesia', 'ASII.JK': 'Astra International',
    }
    all_data = []
    for ticker, nama in tickers.items():
        stock = yf.Ticker(ticker)
        data = stock.history(period="5d")
        if not data.empty:
            for idx, row in data.iterrows():
                dc = row['Close'] - row['Open']
                dcp = (dc / row['Open']) * 100
                all_data.append({
                    'Date': idx.strftime('%Y-%m-%d'), 'Ticker': ticker.replace('.JK', '') if '.JK' in ticker else ticker,
                    'Nama_Saham': nama, 'Open': round(row['Open'], 2), 'High': round(row['High'], 2),
                    'Low': round(row['Low'], 2), 'Close': round(row['Close'], 2), 'Volume': int(row['Volume']),
                    'Daily_Change': round(dc, 2), 'Daily_Change_Pct': round(dcp, 2),
                })
    df = pd.DataFrame(all_data)
    logger.info(f"Scraped {len(df)} rows")
    return df

@task
def upload_to_garage(df):
    client = get_garage_client()
    buckets = [b['Name'] for b in client.list_buckets().get('Buckets', [])]
    if BUCKET not in buckets:
        client.create_bucket(Bucket=BUCKET)

    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    key = f"raw-data/saham_daily_{date_str}.csv"
    client.put_object(Bucket=BUCKET, Key=key, Body=csv_buffer.getvalue().encode('utf-8'))
    logger.info(f"Uploaded to {BUCKET}/{key}")
    return f"s3a://{BUCKET}/{key}"

@task
def spark_batch_etl(minio_path):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, col
    from pyspark.sql.window import Window

    spark = SparkSession.builder \
        .appName("StockBatchETL") \
        .config("spark.hadoop.fs.s3a.endpoint", GARAGE_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", GARAGE_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", GARAGE_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "garage") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "gzip") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    df = spark.read.csv(minio_path, header=True, inferSchema=True)
    df.cache()

    w7 = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-6, 0)
    w30 = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-29, 0)

    tf = df.withColumn("SMA_7", avg("Close").over(w7)) \
            .withColumn("SMA_30", avg("Close").over(w30)) \
            .withColumn("Volatility", (col("High") - col("Low")) / col("Open")) \
            .withColumn("Price_Range", col("High") - col("Low"))

    output_path = f"s3a://{BUCKET}/processed-data/features/{datetime.now().strftime('%Y%m%d_%H%M%S')}/"
    tf.write.mode("overwrite").parquet(output_path)
    logger.info(f"Spark ETL done → {output_path}")
    spark.stop()
    return output_path

@task
def data_quality_check():
    client = get_garage_client()
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    latest = sorted(objs.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
    if not latest:
        raise ValueError("No data found in Garage")

    obj = client.get_object(Bucket=BUCKET, Key=latest[0]['Key'])
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    null_counts = {k: int(v) for k, v in df.isnull().sum().items()}
    null_rows = int(df.isnull().any(axis=1).sum())
    report = {
        'total_rows': len(df), 'null_counts': null_counts,
        'null_rows': null_rows, 'null_pct': round(null_rows / max(len(df), 1) * 100, 2),
        'tickers': df['Ticker'].unique().tolist(),
        'date_range': f"{df['Date'].min()} to {df['Date'].max()}",
    }
    logger.info(f"DQ Report: {json.dumps(report, indent=2)}")

    key = f"metadata/data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    client.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(report, indent=2).encode('utf-8'))
    return report

@task
def check_ihsg_6000():
    client = get_garage_client()
    objs = client.list_objects_v2(Bucket=BUCKET, Prefix='raw-data/')
    contents = sorted(objs.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
    all_data = []
    for obj_info in contents[:5]:
        obj = client.get_object(Bucket=BUCKET, Key=obj_info['Key'])
        all_data.append(pd.read_csv(StringIO(obj['Body'].read().decode('utf-8'))))
    if not all_data:
        return "No IHSG data"

    df = pd.concat(all_data)
    ihsg = df[df['Ticker'] == '^JKSE'].sort_values('Date')
    close = ihsg['Close'].values

    consecutive = 0
    for val in close[-20:]:
        consecutive = consecutive + 1 if val >= 6000 else 0

    result = {
        'latest_close': float(close[-1]) if len(close) > 0 else None,
        'consecutive_days_above_6000': consecutive,
        'max_close': float(close.max()) if len(close) > 0 else None,
    }
    logger.info(f"IHSG: {json.dumps(result)}")
    return result

@flow(log_prints=True)
def batch_flow():
    try:
        check_environment()
        df = scrape_stock_data()
        path = upload_to_garage(df)
        spark_batch_etl(path)
        data_quality_check()
        check_ihsg_6000()
    except Exception as e:
        alert_notify_flow(flow_name="Batch Pipeline", error=str(e))
        raise

if __name__ == "__main__":
    batch_flow()
