import os
import json
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from prefect import flow, task

PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://100.71.251.60:4200/api")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "100.122.2.11:9000")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_TOPIC_LOGS = os.environ.get("KAFKA_TOPIC_LOGS", "system-logs")
LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{LOG_DIR}/health_check.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("health_check")


@task
def check_prefect_server() -> dict:
    result = {"service": "prefect-server", "status": "unknown", "latency_ms": 0}
    try:
        start = datetime.now()
        r = requests.get(f"{PREFECT_API_URL}/health", timeout=10)
        elapsed = (datetime.now() - start).total_seconds() * 1000
        result["latency_ms"] = round(elapsed, 2)
        if r.status_code == 200:
            result["status"] = "healthy"
        else:
            result["status"] = f"unhealthy (HTTP {r.status_code})"
    except Exception as e:
        result["status"] = f"error: {str(e)}"
    logger.info(f"Prefect Server: {result['status']} ({result['latency_ms']}ms)")
    return result


@task
def check_minio() -> dict:
    result = {"service": "minio", "status": "unknown", "latency_ms": 0}
    try:
        start = datetime.now()
        r = requests.get(f"http://{MINIO_ENDPOINT}/minio/health/live", timeout=10)
        elapsed = (datetime.now() - start).total_seconds() * 1000
        result["latency_ms"] = round(elapsed, 2)
        if r.status_code == 200:
            result["status"] = "healthy"
        else:
            result["status"] = f"unhealthy (HTTP {r.status_code})"
    except Exception as e:
        result["status"] = f"error: {str(e)}"
    logger.info(f"MinIO: {result['status']} ({result['latency_ms']}ms)")
    return result


@task
def check_kafka() -> dict:
    result = {"service": "kafka", "status": "unknown", "latency_ms": 0}
    try:
        start = datetime.now()
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        future = producer.send(KAFKA_TOPIC_LOGS, {"health_check": True, "timestamp": datetime.now(timezone.utc).isoformat()})
        future.get(timeout=10)
        producer.flush()
        producer.close()
        elapsed = (datetime.now() - start).total_seconds() * 1000
        result["latency_ms"] = round(elapsed, 2)
        result["status"] = "healthy"
    except Exception as e:
        result["status"] = f"error: {str(e)}"
    logger.info(f"Kafka: {result['status']} ({result['latency_ms']}ms)")
    return result


@flow(name="health-check")
def health_check_flow():
    logger.info("=== Health Check dimulai ===")

    server_status = check_prefect_server()
    minio_status = check_minio()
    kafka_status = check_kafka()

    all_healthy = all(
        s["status"] == "healthy"
        for s in [server_status, minio_status, kafka_status]
    )

    logger.info(f"Overall: {'✓ ALL HEALTHY' if all_healthy else '✗ SOME ISSUES'}")
    logger.info("=== Health Check selesai ===")

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall": "healthy" if all_healthy else "degraded",
        "checks": [server_status, minio_status, kafka_status]
    }
