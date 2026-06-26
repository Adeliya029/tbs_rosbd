from dotenv import load_dotenv
load_dotenv()

import os

# MinIO (Laptop 2)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "100.122.2.11:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MODEL_BUCKET = os.getenv("MODEL_BUCKET", "trained-models")

# PostgreSQL (Laptop 3)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "seismic_db")
PG_USER = os.getenv("PG_USER", "rosbd")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rosbd123")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# FastAPI
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# Alert thresholds
RISK_CRITICAL_THRESHOLD = 0.8
ANOMALY_WARNING_THRESHOLD = 0.5
