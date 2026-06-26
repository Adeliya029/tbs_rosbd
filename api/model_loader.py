import io
import joblib
import boto3
from botocore.config import Config as BotoConfig

import api.config as cfg

_rf_model = None
_iso_model = None


def _create_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{cfg.MINIO_ENDPOINT}",
        aws_access_key_id=cfg.MINIO_ACCESS_KEY,
        aws_secret_access_key=cfg.MINIO_SECRET_KEY,
        config=BotoConfig(
            connect_timeout=5,
            read_timeout=10,
            retries={"max_attempts": 1},
        ),
    )


def load_models():
    global _rf_model, _iso_model

    if _rf_model is not None and _iso_model is not None:
        return _rf_model, _iso_model

    print(f"Loading models from MinIO ({cfg.MINIO_ENDPOINT})...")

    client = _create_client()

    rf_bytes = io.BytesIO()
    client.download_fileobj(cfg.MODEL_BUCKET, "random_forest.pkl", rf_bytes)
    rf_bytes.seek(0)
    _rf_model = joblib.load(rf_bytes)
    print("  random_forest.pkl loaded")

    iso_bytes = io.BytesIO()
    client.download_fileobj(cfg.MODEL_BUCKET, "isolation_forest.pkl", iso_bytes)
    iso_bytes.seek(0)
    _iso_model = joblib.load(iso_bytes)
    print("  isolation_forest.pkl loaded")

    print("All models loaded from MinIO")
    return _rf_model, _iso_model


def get_models():
    global _rf_model, _iso_model
    if _rf_model is None or _iso_model is None:
        return load_models()
    return _rf_model, _iso_model
