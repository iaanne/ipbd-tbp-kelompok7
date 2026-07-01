import os
import logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

GARAGE_ENDPOINT = os.getenv('GARAGE_ENDPOINT', 'http://garage:3900')
GARAGE_ACCESS_KEY = os.getenv('GARAGE_ACCESS_KEY', 'GKc98624849db70446555a905b')
GARAGE_SECRET_KEY = os.getenv('GARAGE_SECRET_KEY', '934f97fb29df4f1da215e689c57ab5b42c4e42798841961e4df77d4d3ae6c828')
BUCKET = os.getenv('GARAGE_BUCKET', 'stock-bucket')
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

spark = SparkSession.builder \
    .appName("StockStreamProcessing") \
    .config("spark.hadoop.fs.s3a.endpoint", GARAGE_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", GARAGE_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", GARAGE_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True)
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", "stock-stream-topic") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

df_agg = df_parsed \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("ticker")
    ) \
    .agg(
        avg("close").alias("avg_close"),
        max("close").alias("max_close"),
        min("close").alias("min_close"),
        avg("open").alias("avg_open"),
        sum("volume").alias("total_volume")
    )

date_str = datetime.now().strftime('%Y%m%d')

def write_to_garage(df, epoch_id):
    df.show(truncate=False)
    df.write.mode("append").parquet(f"s3a://{BUCKET}/stream-data/{date_str}/")

query = df_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_garage) \
    .trigger(processingTime="30 seconds") \
    .start()

print(f"Stream processing berjalan... Output ke s3a://{BUCKET}/stream-data/{date_str}/")
query.awaitTermination()
