"""
Seed real data from MinIO (Laptop 2) into FastAPI / PostgreSQL.

Priority:
  1. processed-features/snapshot_24h/   (pre-computed features, parquet)
  2. processed-features/realtime/        (pre-computed realtime, parquet)
  3. processed-features/historical/      (training dataset, parquet)
  4. raw-earthquake/earthquake-events/   (raw JSON events)
  5. Synthetic fallback
"""

import io
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pandas as pd
import boto3
from botocore.config import Config as BotoConfig

# ── Config ────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "100.122.2.11:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000/api/predict")

# ── MinIO ─────────────────────────────────────────────

def get_client():
    return boto3.client(
        "s3",
        endpoint_url=("https" if MINIO_SECURE else "http") + f"://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(connect_timeout=5, read_timeout=30, retries={"max_attempts": 1}),
    )


def list_files(client, bucket, prefix, ext=None):
    result = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if ext is None or key.endswith(ext):
                result.append(key)
    return result


def read_parquet(client, bucket, key):
    import pyarrow.parquet as pq
    buffer = io.BytesIO()
    client.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return pq.read_table(buffer).to_pandas()


# ── Data Source 1: snapshot_24h (parquet) ────────────

def load_snapshot_features(client):
    bucket = "processed-features"
    prefix = "snapshot_24h/"
    files = list_files(client, bucket, prefix, ext=".parquet")
    if not files:
        print("  No snapshot_24h files found")
        return None

    print(f"  Found {len(files)} snapshot parquet files")
    dfs = []
    for f in files:
        try:
            df = read_parquet(client, bucket, f)
            if len(df) > 0:
                dfs.append(df)
                print(f"    {f.split('/')[-1]}: {len(df)} rows")
        except Exception as e:
            print(f"    {f.split('/')[-1]}: SKIP - {e}")

    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


# ── Data Source 2: realtime (parquet) ─────────────────

def load_realtime_features(client):
    bucket = "processed-features"
    prefix = "realtime/"
    files = list_files(client, bucket, prefix, ext=".parquet")
    if not files:
        print("  No realtime files found")
        return None

    print(f"  Found {len(files)} realtime parquet files")
    dfs = []
    for f in files:
        try:
            df = read_parquet(client, bucket, f)
            if len(df) > 0:
                dfs.append(df)
                print(f"    {f.split('/')[-1]}: {len(df)} rows")
        except Exception as e:
            print(f"    {f.split('/')[-1]}: SKIP - {e}")

    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


# ── Data Source 3: historical training dataset ────────

def load_historical_features(client):
    bucket = "processed-features"
    key = "historical/training_dataset.parquet"
    try:
        df = read_parquet(client, bucket, key)
        print(f"  training_dataset.parquet: {len(df)} rows")
        return df
    except Exception as e:
        print(f"  training_dataset.parquet: FAIL - {e}")
        return None


# ── Data Source 4: Raw JSON from raw-earthquake ──────

def load_raw_events(client):
    bucket = "raw-earthquake"
    prefix = "earthquake-events/"
    files = list_files(client, bucket, prefix, ext=".json")
    if not files:
        print("  No JSON files found in raw-earthquake")
        return None

    print(f"  Found {len(files)} JSON files in raw-earthquake")
    records = []
    for f in files:
        try:
            obj = client.get_object(Bucket=bucket, Key=f)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                records.append(data)
        except Exception as e:
            pass

    if not records:
        return None
    df = pd.DataFrame(records)
    print(f"  Loaded {len(df)} raw events")
    return df


# ── Feature Engineering (raw events → features) ───────

RAW_FEATURE_MAP = {
    "magnitude": "magnitude",
    "depth_km": "depth",
    "latitude": "latitude",
    "longitude": "longitude",
    "region": "region",
    "event_time": "datetime",
    "felt_intensity": "felt_intensity",
}


def compute_features(df):
    if "event_time" not in df.columns:
        for col in ["datetime", "timestamp", "event_time"]:
            if col in df.columns:
                df["event_time"] = pd.to_datetime(df[col], errors="coerce")
                break
        else:
            df["event_time"] = datetime.now(timezone.utc)

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df = df.dropna(subset=["event_time", "region", "magnitude"])
    if "event_id" in df.columns:
        df = df.drop_duplicates(subset=["event_id"])

    if "depth_km" not in df.columns:
        for col in ["depth_km", "depth"]:
            if col in df.columns:
                df["depth_km"] = pd.to_numeric(df[col], errors="coerce")
                break
        else:
            df["depth_km"] = 30.0
    df["depth_km"] = pd.to_numeric(df["depth_km"], errors="coerce").fillna(30)

    df["is_shallow"] = (df["depth_km"] < 70).astype(int)
    if "felt_intensity" in df.columns:
        df["is_felt"] = df["felt_intensity"].notna() & (df["felt_intensity"] != "")
    else:
        df["is_felt"] = 0

    now = df["event_time"].max()
    features = []

    for region_name, grp in df.groupby("region"):
        last_24h = grp[grp["event_time"] >= now - pd.Timedelta(hours=24)]
        if last_24h.empty:
            last_24h = grp.tail(10)

        payload = {
            "event_time": str(now),
            "region": region_name,
            "latitude": float(last_24h["latitude"].mean()),
            "longitude": float(last_24h["longitude"].mean()),
            "magnitude": float(last_24h["magnitude"].max()),
            "depth_km": float(last_24h["depth_km"].mean()),
            "quake_count_24h": float(len(last_24h)),
            "avg_magnitude_24h": float(last_24h["magnitude"].mean()),
            "max_magnitude_24h": float(last_24h["magnitude"].max()),
            "shallow_quake_ratio_24h": float(last_24h["is_shallow"].mean()),
            "felt_count_24h": float(last_24h["is_felt"].sum()),
            "swarm_density": 0.0,
        }
        features.append(payload)

    return features


# ── Synthetic fallback ────────────────────────────────

REGIONS = [
    ("Jawa Barat", -6.9, 107.6),
    ("Jawa Timur", -7.5, 112.5),
    ("Sumatera Barat", -0.9, 100.4),
    ("Sulawesi Tengah", -1.5, 121.0),
    ("NTB", -8.6, 116.5),
    ("Aceh", 4.7, 96.7),
    ("Banten", -6.4, 105.9),
    ("Yogyakarta", -7.8, 110.4),
]


def generate_synthetic():
    import random
    random.seed(42)

    rows = []
    now = datetime.now(timezone.utc)
    for rname, lat, lon in REGIONS:
        for i in range(random.randint(5, 30)):
            rows.append({
                "event_id": f"syn_{rname[:3]}_{i}",
                "datetime": now,
                "latitude": round(lat + random.uniform(-1, 1), 4),
                "longitude": round(lon + random.uniform(-1, 1), 4),
                "magnitude": round(random.uniform(2.0, 6.5), 1),
                "depth": random.choice([10, 20, 30, 50, 80, 100, 150]),
                "region": rname,
                "felt_intensity": random.choice(["", "II", "III", "IV MMI", "V MMI"]),
            })
    return pd.DataFrame(rows)


# ── POST to FastAPI ──────────────────────────────────

def send(payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        FASTAPI_URL, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            r = payload["region"][:30]
            a = body.get("anomaly_score", 0)
            print(f"  OK {r}: class={body['risk_class']} prob={body['risk_probability']:.3f} anomaly={a:.3f}")
            return True
    except urllib.error.HTTPError as e:
        print(f"  FAIL {payload['region'][:30]}: HTTP {e.code}")
        return False
    except Exception as e:
        print(f"  FAIL {payload['region'][:30]}: {e}")
        return False


# ── Main ─────────────────────────────────────────────

def main():
    print("TBS-ROSBD Seed — MinIO to FastAPI")
    print("=" * 45)

    client = get_client()

    # Try each source in priority order
    df = None
    source = ""

    print("\n[1] snapshot_24h (pre-computed features)...")
    df = load_snapshot_features(client)
    if df is not None and len(df) > 0:
        source = "snapshot_24h"

    if df is None or len(df) == 0:
        print("\n[2] realtime (pre-computed features)...")
        df = load_realtime_features(client)
        if df is not None and len(df) > 0:
            source = "realtime"

    if df is None or len(df) == 0:
        print("\n[3] historical training dataset...")
        df = load_historical_features(client)
        if df is not None and len(df) > 0:
            source = "historical"

    if df is None or len(df) == 0:
        print("\n[4] raw JSON events (need feature engineering)...")
        raw = load_raw_events(client)
        if raw is not None and len(raw) > 0:
            features = compute_features(raw)
            if features:
                source = "raw_events"
                # Build payloads list here
                ok = 0
                print(f"\n  Computed {len(features)} feature vectors")
                print(f"\nSending to {FASTAPI_URL}...")
                for p in features:
                    if send(p):
                        ok += 1
                print(f"\nDone: {ok}/{len(features)} sent")
                return

    if df is None or len(df) == 0:
        print("\n[5] Synthetic fallback...")
        df = generate_synthetic()
        source = "synthetic"
        print(f"  Generated {len(df)} events")

    # If we got pre-computed features from parquet
    if source in ("snapshot_24h", "realtime"):
        print(f"  Loaded {len(df)} rows")
        # Map columns to payload
        required_cols = {
            "event_time", "region", "latitude", "longitude",
            "magnitude", "depth_km", "quake_count_24h",
            "avg_magnitude_24h", "max_magnitude_24h",
            "shallow_quake_ratio_24h", "felt_count_24h", "swarm_density",
        }
        missing = required_cols - set(df.columns)
        if missing:
            print(f"  Missing columns: {missing}, falling back to synthetic")
            df = generate_synthetic()
            source = "synthetic"

        # Convert event_time to string
        df["event_time"] = df["event_time"].astype(str)

        payloads = df[list(required_cols)].to_dict(orient="records")
        ok = 0
        print(f"\nSending {len(payloads)} rows to {FASTAPI_URL}...")
        for p in payloads:
            if send(p):
                ok += 1
        print(f"\nDone: {ok}/{len(payloads)} sent")

    elif source == "historical":
        print(f"  Loaded {len(df)} rows")
        features = compute_features(df)
        ok = 0
        print(f"\n  Computed {len(features)} feature vectors")
        print(f"\nSending to {FASTAPI_URL}...")
        for p in features:
            if send(p):
                ok += 1
        print(f"\nDone: {ok}/{len(features)} sent")

    elif source == "synthetic":
        features = compute_features(df)
        ok = 0
        print(f"\n  Computed {len(features)} feature vectors")
        print(f"\nSending to {FASTAPI_URL}...")
        for p in features:
            if send(p):
                ok += 1
        print(f"\nDone: {ok}/{len(features)} sent")


if __name__ == "__main__":
    main()
