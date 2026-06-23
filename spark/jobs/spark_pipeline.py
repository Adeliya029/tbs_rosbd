import json
import urllib.request
import urllib.error

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    when,
    lit,
    trim,
    current_timestamp,
    max as spark_max,
    avg,
    count,
    sum as spark_sum,
    expr
)


# =========================
# CONFIG
# =========================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"

RAW_PATH = "s3a://raw-earthquake/earthquake-events/"
PROCESSED_PATH = "s3a://processed-features/events/"
SNAPSHOT_PATH = "s3a://processed-features/snapshot_24h/"

# Ganti kalau endpoint FastAPI Laptop 3 berbeda
FASTAPI_URL = "http://100.71.251.60:8000/api/inference/risk-score"


# =========================
# SPARK SESSION
# =========================

spark = (
    SparkSession.builder
    .appName("full-spark-earthquake-pipeline")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .getOrCreate()
)


def send_payload_to_fastapi(payload: dict):

    data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        FASTAPI_URL,
        data=data,
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    try:

        with urllib.request.urlopen(
            request,
            timeout=15
        ) as response:

            body = response.read().decode("utf-8")

            print(
                f"[FASTAPI] SUCCESS "
                f"status={response.status}"
            )

            print(body)

    except urllib.error.HTTPError as e:

        print(
            f"[FASTAPI] HTTP ERROR "
            f"status={e.code}"
        )

        print(
            e.read().decode("utf-8")
        )

    except Exception as e:

        print(
            f"[FASTAPI] CONNECTION ERROR: {str(e)}"
        )


try:
    # =========================
    # 1. READ RAW JSON
    # =========================

    print("\n=== STEP 1: READ RAW JSON FROM MINIO ===")

    raw_df = spark.read.json(RAW_PATH)

    print("Schema raw:")
    raw_df.printSchema()

    print("Preview raw:")
    raw_df.show(20, truncate=False)

    raw_count = raw_df.count()
    print("Jumlah raw:", raw_count)

    if raw_count == 0:
        print("Raw data kosong. Pipeline dihentikan.")
        spark.stop()
        exit()


    # =========================
    # 2. PREPROCESSING
    # =========================

    print("\n=== STEP 2: PREPROCESSING RAW DATA ===")

    processed_df = (
        raw_df
        .withColumn("event_time", to_timestamp(col("datetime")))
        .withColumn("depth_km", col("depth").cast("double"))
        .withColumn("magnitude", col("magnitude").cast("double"))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))

        .withColumn(
            "is_shallow",
            when(col("depth_km") < 70, lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "is_felt",
            when(
                (col("felt_intensity").isNotNull()) &
                (trim(col("felt_intensity")) != ""),
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn("processed_at", current_timestamp())
        .withColumn(
            "event_date",
            expr("date(event_time)")
        )

        .select(
            "event_id",
            "event_time",
            "region",
            "latitude",
            "longitude",
            "magnitude",
            "depth_km",
            "felt_intensity",
            "potensi",
            "shakemap",
            "source",
            "is_shallow",
            "is_felt",
            "processed_at",
            "event_date"
        )

        .dropna(subset=[
            "event_id",
            "event_time",
            "latitude",
            "longitude",
            "magnitude",
            "depth_km"
        ])

        .dropDuplicates(["event_id"])
    )

    print("Schema processed:")
    processed_df.printSchema()

    print("Preview processed:")
    processed_df.show(20, truncate=False)

    processed_count = processed_df.count()
    print("Jumlah setelah cleaning dan deduplicate:", processed_count)

    if processed_count == 0:
        print("Processed data kosong. Pipeline dihentikan.")
        spark.stop()
        exit()


    # =========================
    # 3. WRITE PROCESSED FEATURES
    # =========================

    print("\n=== STEP 3: WRITE PROCESSED FEATURES TO MINIO ===")

    processed_df.write \
        .mode("append") \
        .partitionBy("event_date") \
        .parquet(PROCESSED_PATH)

    print("Processed features berhasil disimpan ke:", PROCESSED_PATH)


    # =========================
    # 4. BUILD SNAPSHOT 24H
    # =========================

    print("\n=== STEP 4: BUILD SNAPSHOT 24H ===")

    latest_time = processed_df.agg(
        spark_max("event_time").alias("latest_time")
    ).collect()[0]["latest_time"]

    print("Latest event_time:", latest_time)

    last_24h_df = processed_df.filter(
        col("event_time") >= expr(f"timestamp('{latest_time}') - interval 24 hours")
    )

    print("Data dalam window 24 jam terakhir:")
    last_24h_df.show(20, truncate=False)

    snapshot_df = (
        last_24h_df
        .groupBy("region")
        .agg(
            count("*").alias("quake_count_24h"),
            avg("magnitude").alias("avg_magnitude_24h"),
            spark_max("magnitude").alias("max_magnitude_24h"),
            avg("is_shallow").alias("shallow_quake_ratio_24h"),
            spark_sum("is_felt").alias("felt_count_24h"),
            avg("latitude").alias("latitude"),
            avg("longitude").alias("longitude"),
            avg("depth_km").alias("depth_km"),
            spark_max("event_time").alias("event_time")
        )
        .withColumn("swarm_density", lit(0.0))
        .withColumn("magnitude", col("max_magnitude_24h"))
        .withColumn("snapshot_created_at", current_timestamp())
        .withColumn("snapshot_date", expr("current_date()"))
    )

    print("Schema snapshot:")
    snapshot_df.printSchema()

    print("Preview snapshot:")
    snapshot_df.show(20, truncate=False)

    snapshot_count = snapshot_df.count()
    print("Jumlah snapshot:", snapshot_count)

    if snapshot_count == 0:
        print("Snapshot kosong. Pipeline dihentikan.")
        spark.stop()
        exit()


    # =========================
    # 5. WRITE SNAPSHOT TO MINIO
    # =========================

    print("\n=== STEP 5: WRITE SNAPSHOT TO MINIO ===")

    snapshot_df.write \
        .mode("append") \
        .partitionBy("snapshot_date") \
        .parquet(SNAPSHOT_PATH)

    print("Snapshot 24 jam berhasil disimpan ke:", SNAPSHOT_PATH)


    # =========================
    # 6. SEND SNAPSHOT TO FASTAPI
    # =========================

    print("\n=== STEP 6: SEND SNAPSHOT TO FASTAPI ===")

    rows = snapshot_df.collect()

    for row in rows:
        payload = {
            "event_time": str(row["event_time"]),
            "region": row["region"],
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "magnitude": float(row["magnitude"]),
            "depth_km": float(row["depth_km"]),
            "quake_count_24h": float(row["quake_count_24h"]),
            "avg_magnitude_24h": float(row["avg_magnitude_24h"]),
            "max_magnitude_24h": float(row["max_magnitude_24h"]),
            "shallow_quake_ratio_24h": float(row["shallow_quake_ratio_24h"]),
            "felt_count_24h": float(row["felt_count_24h"]),
            "swarm_density": float(row["swarm_density"]),
        }

        print("\nMengirim payload ke FastAPI:")
        print(json.dumps(payload, indent=2))

        send_payload_to_fastapi(payload)

    print("\n=== FULL SPARK PIPELINE SELESAI ===")


finally:
    spark.stop()