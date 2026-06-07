"""
Spark Job: Konsumsi data dari Kafka dan simpan ke MinIO
Alur: Kafka → Spark Streaming → MinIO (raw)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Konfigurasi
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "rosbd_kafka:29092")
KAFKA_TOPIC_EARTHQUAKE = os.getenv("KAFKA_TOPIC_EARTHQUAKE", "earthquake-events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "rosbd_minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")

BUCKET_RAW = "raw-earthquake"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/earthquake-stream"

# Schema untuk data gempa
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
    """Main Spark streaming job."""
    
    # Buat Spark session dengan config MinIO
    spark = SparkSession.builder \
        .appName("KafkaToMinIO-Earthquake") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 50)
    print("Spark Job: Kafka → MinIO Earthquake Pipeline")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_EARTHQUAKE}")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print("=" * 50)
    
    # Baca streaming dari Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_EARTHQUAKE) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON dari Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), earthquake_schema).alias("data")
    ).select("data.*")
    
    # Tampilkan sample data (untuk debugging)
    query_debug = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Write ke MinIO dalam format Parquet (lebih efisien)
    query_minio = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"s3a://{BUCKET_RAW}/earthquake-events/") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/minio") \
        .trigger(processingTime="30 seconds") \
        .partitionBy("source") \
        .start()
    
    print("Streaming started. Waiting for data...")
    
    # Tunggu sampai dihentikan
    query_debug.awaitTermination()
    query_minio.awaitTermination()

if __name__ == "__main__":
    main()