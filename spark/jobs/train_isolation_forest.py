import io
import joblib
import boto3
import numpy as np
import pandas as pd

from sklearn.ensemble import IsolationForest


MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"

DATA_BUCKET = "processed-features"
MODEL_BUCKET = "trained-models"

DATA_FILE = "historical/training_dataset.parquet"

FEATURES = [
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

    predictions = model.predict(X)

    anomaly_scores = -model.score_samples(X)
    anomaly_count = int((predictions == -1).sum())
    normal_count = int((predictions == 1).sum())

    print(f"\nEvaluation:")
    print(f"  Samples          : {len(X)}")
    print(f"  Anomalies (-1)   : {anomaly_count} ({anomaly_count / len(X) * 100:.2f}%)")
    print(f"  Normal    (+1)   : {normal_count} ({normal_count / len(X) * 100:.2f}%)")
    print(f"  Anomaly score:")
    print(f"    min  : {anomaly_scores.min():.4f}")
    print(f"    max  : {anomaly_scores.max():.4f}")
    print(f"    mean : {anomaly_scores.mean():.4f}")
    print(f"    std  : {anomaly_scores.std():.4f}")
    print(f"    P90  : {np.percentile(anomaly_scores, 90):.4f}")
    print(f"    P95  : {np.percentile(anomaly_scores, 95):.4f}")
    print(f"    P99  : {np.percentile(anomaly_scores, 99):.4f}")

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