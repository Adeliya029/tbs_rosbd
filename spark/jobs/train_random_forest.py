import io
import joblib
import boto3
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


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
    "felt_count_24h",
    "swarm_density",
    "latitude",
    "longitude"
]

TARGET = "risk_level"


def main():

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    print("Downloading dataset...")

    dataset_buffer = io.BytesIO()

    client.download_fileobj(
        DATA_BUCKET,
        DATA_FILE,
        dataset_buffer
    )

    dataset_buffer.seek(0)

    df = pd.read_parquet(dataset_buffer)

    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    print("Training Random Forest...")

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=15,
        random_state=42,
        class_weight="balanced"
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    print(
        "Accuracy:",
        accuracy_score(y_test, predictions)
    )

    model_buffer = io.BytesIO()

    joblib.dump(
        model,
        model_buffer
    )

    model_buffer.seek(0)

    client.upload_fileobj(
        model_buffer,
        MODEL_BUCKET,
        "random_forest.pkl"
    )

    print("Uploaded random_forest.pkl")
    

if __name__ == "__main__":
    main()