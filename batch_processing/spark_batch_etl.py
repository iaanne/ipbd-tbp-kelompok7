from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("StockBatchETL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "SuperSecretPassword123!") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# baca data historis dari MinIO (BATCH!)
df = spark.read.csv(
    "s3a://stock-batch-bucket/raw-data/saham_indonesia_historical.csv",
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
    "s3a://stock-batch-bucket/processed-data/features/"
)
print("✅ Data batch tersimpan di processed-data/")
