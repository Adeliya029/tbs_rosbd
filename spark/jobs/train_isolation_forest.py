import io
import joblib
import boto3
import pandas as pd

from sklearn.ensemble import IsolationForest


MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"

DATA_BUCKET = "processed-features"
MODEL_BUCKET = "trained-models"

DATA_FILE = "historical/training_dataset.parquet"

FEATURES = [
    "magnitude",
    "depth_km",
    "quake_count_24h",
    "avg_magnitude_24h",
    "max_magnitude_24h",
    "shallow_quake_ratio_24h",
    "latitude",
    "longitude"
]


def main():

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    dataset_buffer = io.BytesIO()

    client.download_fileobj(
        DATA_BUCKET,
        DATA_FILE,
        dataset_buffer
    )

    dataset_buffer.seek(0)

    df = pd.read_parquet(dataset_buffer)

    X = df[FEATURES]

    print("Training Isolation Forest...")

    model = IsolationForest(
        n_estimators=300,
        contamination=0.02,
        random_state=42
    )

    model.fit(X)

    model_buffer = io.BytesIO()

    joblib.dump(
        model,
        model_buffer
    )

    model_buffer.seek(0)

    client.upload_fileobj(
        model_buffer,
        MODEL_BUCKET,
        "isolation_forest.pkl"
    )

    print("Uploaded isolation_forest.pkl")


if __name__ == "__main__":
    main()