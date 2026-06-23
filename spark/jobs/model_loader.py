import io
import joblib
import boto3


MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"

BUCKET = "trained-models"


_rf_model = None
_iso_model = None


def load_models():

    global _rf_model
    global _iso_model

    if _rf_model is not None and _iso_model is not None:
        return _rf_model, _iso_model

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    rf_bytes = io.BytesIO()

    client.download_fileobj(
        BUCKET,
        "random_forest.pkl",
        rf_bytes
    )

    rf_bytes.seek(0)

    _rf_model = joblib.load(rf_bytes)

    iso_bytes = io.BytesIO()

    client.download_fileobj(
        BUCKET,
        "isolation_forest.pkl",
        iso_bytes
    )

    iso_bytes.seek(0)

    _iso_model = joblib.load(iso_bytes)

    print("Models loaded from MinIO")

    return _rf_model, _iso_model
