import os
import json
import io
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "rosbd_kafka:29092")
KAFKA_TOPIC_EARTHQUAKE = os.getenv("KAFKA_TOPIC_EARTHQUAKE", "earthquake-events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "rosbd_minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")

BUCKET_RAW = "raw-earthquake"
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


def upload_to_minio(df, epoch_id):
    rows = df.collect()
    for row in rows:
        data = row.asDict()
        event_id = data.get("event_id", "unknown")
        filename = f"earthquake-events/{event_id}.json"
        content = json.dumps(data, default=str).encode("utf-8")

        url = f"http://{MINIO_ENDPOINT}/{BUCKET_RAW}/{filename}"
        req = urllib.request.Request(url, data=content, method="PUT")
        req.add_header("Content-Type", "application/json")

        password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_manager.add_password(None, url, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_manager)
        opener = urllib.request.build_opener(auth_handler)
        try:
            opener.open(req)
            print(f"  Uploaded: {filename}")
        except Exception as e:
            print(f"  Failed: {filename} - {e}")


def main():
    spark = SparkSession.builder \
        .appName("KafkaToMinIO-Earthquake") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 50)
    print("Spark Job: Kafka to MinIO Earthquake Pipeline")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_EARTHQUAKE}")
    print(f"MinIO: {MINIO_ENDPOINT}")
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
        .foreachBatch(upload_to_minio) \
        .trigger(processingTime="30 seconds") \
        .start()

    print("Streaming started. Waiting for data...")
    query_debug.awaitTermination()
    query_minio.awaitTermination()


if __name__ == "__main__":
    main()
