import os
import json
import time
import io
import logging
import warnings
from datetime import datetime, timezone
from kafka import KafkaConsumer
from minio import Minio

warnings.filterwarnings("ignore", message=".*socks5_proxy.*")
warnings.filterwarnings("ignore", message=".*value_deserializer.*")

logging.basicConfig(level=logging.WARNING, format="%(asctime)s | %(message)s")
logger = logging.getLogger("direct_consumer")
logger.setLevel(logging.INFO)

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "100.76.33.80:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_EARTHQUAKE", "earthquake-events")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "False").lower() == "true"

BUCKET_RAW = "raw-earthquake"

def main():
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    if not minio_client.bucket_exists(BUCKET_RAW):
        minio_client.make_bucket(BUCKET_RAW)
        logger.info(f"Created bucket: {BUCKET_RAW}")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id="laptop2-direct-consumer"
    )

    logger.info(f"Connected to Kafka: {KAFKA_SERVERS}")
    logger.info(f"Listening topic: {KAFKA_TOPIC}")
    logger.info("Waiting for data...")

    for message in consumer:
        data = message.value
        event_id = data.get("event_id", f"unknown-{message.offset}")
        mag = data.get("magnitude", "?")
        region = data.get("region", "?")
        ts = data.get("event_time", data.get("datetime", "?"))
        depth = data.get("depth_km", data.get("depth", "?"))

        print(f"\n{'='*60}")
        print(f"  [DATA MASUK] Offset {message.offset}")
        print(f"  Event ID  : {event_id}")
        print(f"  Magnitude : {mag}")
        print(f"  Region    : {region}")
        print(f"  Depth     : {depth} km")
        print(f"  Waktu     : {ts}")
        print(f"{'='*60}")

        filename = f"earthquake-events/{event_id}_{message.offset}.json"
        content = json.dumps(data, default=str)

        try:
            buffer = io.BytesIO(content.encode("utf-8"))
            minio_client.put_object(
                BUCKET_RAW, filename,
                data=buffer,
                length=len(content.encode("utf-8")),
                content_type="application/json"
            )
            print(f"  [SIMPAN KE MINIO] {filename} ({len(content)} bytes)")
        except Exception as e:
            print(f"  [GAGAL] Upload: {e}")
            logger.error(f"Failed to upload {filename}: {e}")

if __name__ == "__main__":
    main()
