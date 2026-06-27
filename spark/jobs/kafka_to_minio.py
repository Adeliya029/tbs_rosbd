import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "100.76.33.80:9093")
KAFKA_TOPIC_EARTHQUAKE = os.getenv("KAFKA_TOPIC_EARTHQUAKE", "earthquake-events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "rosbd_minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")

S3A_RAW_PATH = "s3a://raw-earthquake/earthquake-events/"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/earthquake-stream"

earthquake_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("depth", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("felt_intensity", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True)
])


def main():
    spark = SparkSession.builder \
        .appName("KafkaToMinIO-Earthquake") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 50)
    print("Spark Job: Kafka to MinIO Earthquake Pipeline")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_EARTHQUAKE}")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Output: {S3A_RAW_PATH}")
    print("=" * 50)

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_EARTHQUAKE) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.select(
        from_json(col("value").cast("string"), earthquake_schema).alias("data")
    ).select("data.*")

    query_debug = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    query_minio = parsed_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", S3A_RAW_PATH) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/minio") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("Streaming started. Waiting for data...")
    query_debug.awaitTermination()
    query_minio.awaitTermination()


if __name__ == "__main__":
    main()
