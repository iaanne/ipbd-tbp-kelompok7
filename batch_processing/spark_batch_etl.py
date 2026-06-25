import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')

spark = SparkSession.builder \
    .appName("StockBatchETL") \
    .config("spark.hadoop.fs.s3a.endpoint", GARAGE_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", GARAGE_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", GARAGE_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# baca data historis dari MinIO (BATCH!)
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')

df = spark.read.csv(
    f"s3a://{BUCKET}/raw-data/saham_indonesia_historical.csv",
    header=True,
    inferSchema=True
)

print(" Data Batch dari MinIO:")
df.show(5)

# TRANSFORMASI BATCH (hitung indikator teknikal)
window_7d = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-6, 0)
window_30d = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-29, 0)

df_transformed = df \
    .withColumn("SMA_7", avg("Close").over(window_7d)) \
    .withColumn("SMA_30", avg("Close").over(window_30d)) \
    .withColumn("Volatility", (col("High") - col("Low")) / col("Open")) \
    .withColumn("Price_Range", col("High") - col("Low"))

# save hasil batch ke MinIO
df_transformed.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/processed-data/features/"
)
print("✅ Data batch tersimpan di processed-data/")
