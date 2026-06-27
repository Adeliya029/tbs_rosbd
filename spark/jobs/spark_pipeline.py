import json
import urllib.request
import urllib.error

from st_dbscan import detect_swarm

from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    StringType,
    DoubleType as SDouble,
    IntegerType
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    regexp_replace,
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

KAFKA_BOOTSTRAP_SERVERS = "100.76.33.80:9093"
KAFKA_TOPIC = "earthquake-events"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"

RAW_PATH = "s3a://raw-earthquake/earthquake-events/"
PROCESSED_PATH = "s3a://processed-features/events/"
SNAPSHOT_PATH = "s3a://processed-features/snapshot_24h/"
CHECKPOINT_PATH = "/opt/spark/checkpoints/earthquake-pipeline"

FASTAPI_URL = "http://100.71.251.60:8000/api/predict"


# =========================
# KAFKA SCHEMA
# (sesuai output realtime_producer.py)
# =========================

kafka_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("latitude", SDouble(), True),
    StructField("longitude", SDouble(), True),
    StructField("magnitude", SDouble(), True),
    StructField("depth", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("felt_intensity", StringType(), True),
    StructField("shakemap", StringType(), True),
    StructField("potensi", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


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
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================
# FASTAPI HELPER
# =========================

def send_payload_to_fastapi(payload: dict):

    data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        FASTAPI_URL,
        data=data,
        headers={"Content-Type": "application/json"},
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

        print(e.read().decode("utf-8"))

    except Exception as e:

        print(
            f"[FASTAPI] CONNECTION ERROR: "
            f"{str(e)}"
        )


# =========================
# MICRO-BATCH PROCESSOR
# =========================

def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: kosong, skip.")
        return

    print(f"\n{'=' * 60}")
    print(f"BATCH {batch_id}")
    print(f"{'=' * 60}")

    # =========================
    # 1. PARSE JSON DARI KAFKA
    # =========================

    parsed_df = (
        batch_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(
            from_json(
                col("json_str"),
                kafka_schema
            ).alias("data")
        )
        .select("data.*")
    )

    incoming = parsed_df.count()
    print(f"Incoming dari Kafka: {incoming} event")

    # =========================
    # 2. WRITE RAW KE MINIO
    # =========================

    parsed_df.write.mode("append").json(RAW_PATH)
    print("Raw event tersimpan ke MinIO")

    # =========================
    # 3. PREPROCESSING
    # =========================

    processed_df = (
        parsed_df
        .withColumn(
            "event_time",
            coalesce(
                to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(
                    regexp_replace(col("datetime"), r" (WIB|WITA|WIT)$", ""),
                    "yyyy-MM-dd HH:mm:ss"
                ),
                to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss")
            )
        )
        .withColumn(
            "depth_km",
            col("depth").cast("double")
        )
        .withColumn(
            "magnitude",
            col("magnitude").cast("double")
        )
        .withColumn(
            "latitude",
            col("latitude").cast("double")
        )
        .withColumn(
            "longitude",
            col("longitude").cast("double")
        )
        .withColumn(
            "is_shallow",
            when(
                col("depth_km") < 70,
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn(
            "is_felt",
            when(
                (col("felt_intensity").isNotNull())
                & (
                    trim(col("felt_intensity"))
                    != ""
                ),
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn(
            "processed_at",
            current_timestamp()
        )
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

    processed_count = processed_df.count()
    print(f"Setelah cleaning: {processed_count} event")

    if processed_count == 0:
        print(
            f"Batch {batch_id}: "
            f"tidak ada data valid, skip."
        )
        return

    # =========================
    # 4. WRITE PROCESSED FEATURES
    # =========================

    processed_df.write \
        .mode("append") \
        .partitionBy("event_date") \
        .parquet(PROCESSED_PATH)

    print("Processed features tersimpan ke MinIO")

    # =========================
    # 5. BUILD 24H SNAPSHOT
    # =========================

    all_processed = spark.read.parquet(PROCESSED_PATH)

    latest_time = all_processed.agg(
        spark_max("event_time").alias("latest_time")
    ).collect()[0]["latest_time"]

    print(f"Latest event_time: {latest_time}")

    last_24h_df = all_processed.filter(
        col("event_time")
        >= expr(
            f"timestamp('{latest_time}') "
            f"- interval 24 hours"
        )
    )

    # =========================
    # 6. ST-DBSCAN PER REGION
    # =========================

    regions = (
        last_24h_df
        .select("region")
        .distinct()
        .collect()
    )

    swarm_map = {}

    for r in regions:

        region_name = r["region"]

        region_pdf = (
            last_24h_df
            .filter(col("region") == region_name)
            .select(
                "event_time",
                "latitude",
                "longitude",
                "magnitude",
                "depth_km"
            )
            .toPandas()
        )

        swarm_result = detect_swarm(region_pdf)

        swarm_map[region_name] = swarm_result

        print(
            f"  Region={region_name}  "
            f"Events={len(region_pdf)}  "
            f"Swarm={swarm_result}"
        )

    swarm_udf = udf(
        lambda r: float(
            swarm_map.get(r, 0.0)
        ),
        DoubleType()
    )

    # =========================
    # 7. SNAPSHOT AGGREGATION
    # =========================

    snapshot_df = (
        last_24h_df
        .groupBy("region")
        .agg(
            count("*").alias("quake_count_24h"),

            avg("magnitude").alias(
                "avg_magnitude_24h"
            ),

            spark_max("magnitude").alias(
                "max_magnitude_24h"
            ),

            avg("is_shallow").alias(
                "shallow_quake_ratio_24h"
            ),

            spark_sum("is_felt").alias(
                "felt_count_24h"
            ),

            avg("latitude").alias("latitude"),

            avg("longitude").alias("longitude"),

            avg("depth_km").alias("depth_km"),

            spark_max("event_time").alias(
                "event_time"
            )
        )
        .withColumn(
            "swarm_density",
            swarm_udf(col("region"))
        )
        .withColumn(
            "snapshot_created_at",
            current_timestamp()
        )
        .withColumn(
            "snapshot_date",
            expr("current_date()")
        )
    )

    snapshot_count = snapshot_df.count()
    print(f"Snapshot regions: {snapshot_count}")

    # =========================
    # 8. WRITE SNAPSHOT KE MINIO
    # =========================

    if snapshot_count > 0:

        snapshot_df.write \
            .mode("append") \
            .partitionBy("snapshot_date") \
            .parquet(SNAPSHOT_PATH)

        print("Snapshot tersimpan ke MinIO")

    # =========================
    # 9. POST KE FASTAPI
    # =========================

    rows = snapshot_df.collect()

    for row in rows:

        payload = {
            "event_time": str(
                row["event_time"]
            ),
            "region": row["region"],
            "latitude": float(
                row["latitude"]
            ),
            "longitude": float(
                row["longitude"]
            ),
            "depth_km": float(
                row["depth_km"]
            ),
            "quake_count_24h": float(
                row["quake_count_24h"]
            ),
            "avg_magnitude_24h": float(
                row["avg_magnitude_24h"]
            ),
            "max_magnitude_24h": float(
                row["max_magnitude_24h"]
            ),
            "shallow_quake_ratio_24h": float(
                row["shallow_quake_ratio_24h"]
            ),
            "felt_count_24h": float(
                row["felt_count_24h"]
            ),
            "swarm_density": float(
                row["swarm_density"]
            ),
        }

        print(
            f"\nMengirim payload "
            f"region={row['region']}:"
        )

        print(
            json.dumps(payload, indent=2)
        )

        send_payload_to_fastapi(payload)

    print(f"Batch {batch_id} selesai.\n")


# =========================
# 10. KAFKA STREAMING READ
# =========================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# =========================
# 11. START STREAMING
# =========================

query = (
    raw_stream.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(processingTime="60 seconds")
    .start()
)

print("=" * 50)
print("SPARK STREAMING PIPELINE STARTED")
print(f"Kafka     : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic     : {KAFKA_TOPIC}")
print(f"FastAPI   : {FASTAPI_URL}")
print(f"Trigger   : 10 detik")
print("=" * 50)
print("Flow: Kafka -> Feature Engineering -> ST-DBSCAN -> MinIO + FastAPI")
print("=" * 50)

query.awaitTermination()
