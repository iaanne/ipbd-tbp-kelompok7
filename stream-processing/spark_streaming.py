# baca data real-time dari Kafka
from pyspark.sql import SparkSession
from pspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("StockStreamProcessing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "SuperSecretPassword123!") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# schema untuk data Kafka (JSON)
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True)
])

# baca stream dari Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-stream-topic") \
    .option("stratingOffsets", "latest") \
    .load()

# parse JSON
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# transformasi StringTypeEAM (real time aggregation)
df_agg = df_parsed \
    .withColumn("event_time", col("timestamp".cast("timestamp")) \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("ticker")
    )\
    .agg(
        avg("close").alias("avg_close"),
        max("close").alias("max_close"),
        min("close").alias("min_close"),
        avg("open").alias("avg_open"),
        sum("volume").alias("total_volume")
    )

# output stream ke console 
query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Stream processing berjalan...")
query.awaitTermination()
