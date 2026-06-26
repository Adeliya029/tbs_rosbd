"""
Background thread: periodically polls MinIO for new parquet files,
processes them through the predict pipeline, and saves to PostgreSQL.

Polls both:
  - processed-features/snapshot_24h/  (pre-computed features → predict)
  - processed-features/realtime/       (raw events → group → feature → predict)
"""

import io
import json
import threading
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig

import api.config as cfg
from api.db import get_db, save_prediction, save_anomaly
from api.inference import predict_snapshot
from api.alert import trigger_alerts

POLL_INTERVAL = 30
TRACKING_FILE = "/tmp/processed_minio_files.json"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=("https" if cfg.MINIO_SECURE else "http") + f"://{cfg.MINIO_ENDPOINT}",
        aws_access_key_id=cfg.MINIO_ACCESS_KEY,
        aws_secret_access_key=cfg.MINIO_SECRET_KEY,
        config=BotoConfig(connect_timeout=5, read_timeout=30, retries={"max_attempts": 1}),
    )


def load_tracking():
    try:
        with open(TRACKING_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_tracking(tracking):
    try:
        with open(TRACKING_FILE, "w") as f:
            json.dump(tracking, f)
    except Exception as e:
        print(f"[POLLER] Gagal simpan tracking: {e}")


def process_row(row, conn):
    features = {
        "region": str(row.get("region", "Unknown")),
        "latitude": float(row.get("latitude", 0)),
        "longitude": float(row.get("longitude", 0)),
        "magnitude": float(row.get("magnitude", 0)),
        "depth_km": float(row.get("depth_km", 30)),
        "quake_count_24h": float(row.get("quake_count_24h", 1)),
        "avg_magnitude_24h": float(row.get("avg_magnitude_24h", row.get("magnitude", 0))),
        "max_magnitude_24h": float(row.get("max_magnitude_24h", row.get("magnitude", 0))),
        "shallow_quake_ratio_24h": float(row.get("shallow_quake_ratio_24h", 0.5)),
        "felt_count_24h": float(row.get("felt_count_24h", 0)),
        "swarm_density": float(row.get("swarm_density", 0)),
        "event_time": str(row.get("event_time", row.get("snapshot_created_at", datetime.now(timezone.utc)))),
    }

    result = predict_snapshot(features)
    if result is None:
        return

    result.update(
        region=features["region"],
        event_time=features["event_time"],
        latitude=features["latitude"],
        longitude=features["longitude"],
        magnitude=features["magnitude"],
        depth_km=features["depth_km"],
        swarm_density=features["swarm_density"],
    )

    try:
        save_prediction(conn, features, result)
        if result["anomaly_score"] > cfg.ANOMALY_WARNING_THRESHOLD:
            save_anomaly(conn, features, result)
        sent = trigger_alerts({**features, **result})
        if sent:
            print(f"[POLLER] Alert sent for {features['region']}")
        print(f"[POLLER] {features['region'][:30]:30s} → {result['risk_class']} (anom={result['anomaly_score']:.3f})")
    except Exception as e:
        print(f"[POLLER] DB/TELEGRAM error for {features.get('region')}: {e}")


def process_events_file(df, conn):
    """Raw events → group by region → compute features → process rows."""
    dt_col = "event_time" if "event_time" in df.columns else "datetime"
    if dt_col not in df.columns:
        print(f"[POLLER] events file: no time column ({df.columns.tolist()})")
        return

    df["_time"] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=["_time", "region", "magnitude"])
    if df.empty:
        return

    depth_col = "depth_km" if "depth_km" in df.columns else "depth"
    df["_depth"] = pd.to_numeric(df.get(depth_col, pd.Series([30] * len(df))), errors="coerce").fillna(30)

    df["_shallow"] = (df["_depth"] < 70).astype(int)
    felt = "felt_intensity" if "felt_intensity" in df.columns else None
    df["_felt"] = df[felt].notna().astype(int) if felt else 0

    now = df["_time"].max()
    for region_name, grp in df.groupby("region"):
        last_24h = grp[grp["_time"] >= now - pd.Timedelta(hours=24)]
        if last_24h.empty:
            last_24h = grp.tail(10)

        row = {
            "region": region_name,
            "latitude": float(last_24h["latitude"].mean()),
            "longitude": float(last_24h["longitude"].mean()),
            "magnitude": float(last_24h["magnitude"].max()),
            "depth_km": float(last_24h["_depth"].mean()),
            "quake_count_24h": float(len(last_24h)),
            "avg_magnitude_24h": float(last_24h["magnitude"].mean()),
            "max_magnitude_24h": float(last_24h["magnitude"].max()),
            "shallow_quake_ratio_24h": float(last_24h["_shallow"].mean()),
            "felt_count_24h": float(last_24h["_felt"].sum()),
            "swarm_density": 0.0,
            "event_time": str(now),
        }
        process_row(row, conn)


def poll_once(client, conn):
    tracking = load_tracking()
    found = False

    # 1) Snapshot — features ready
    for page in client.get_paginator("list_objects_v2").paginate(Bucket="processed-features", Prefix="snapshot_24h/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            fid = f"processed-features/{key}"
            etag = obj.get("ETag", "")
            if tracking.get(fid) == etag:
                continue

            print(f"[POLLER] New snapshot: {key}")
            buf = io.BytesIO()
            client.download_fileobj("processed-features", key, buf)
            buf.seek(0)
            df = pq.read_table(buf).to_pandas()
            if not df.empty:
                for _, row in df.iterrows():
                    try:
                        process_row(row.to_dict(), conn)
                    except Exception as e:
                        print(f"[POLLER] Row error: {e}")
            tracking[fid] = etag
            found = True

    # 2) Realtime — raw events
    for page in client.get_paginator("list_objects_v2").paginate(Bucket="processed-features", Prefix="realtime/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            fid = f"processed-features/{key}"
            etag = obj.get("ETag", "")
            if tracking.get(fid) == etag:
                continue

            print(f"[POLLER] New realtime: {key}")
            buf = io.BytesIO()
            client.download_fileobj("processed-features", key, buf)
            buf.seek(0)
            df = pq.read_table(buf).to_pandas()
            if not df.empty:
                process_events_file(df, conn)
            tracking[fid] = etag
            found = True

    if found:
        save_tracking(tracking)


def poller_loop():
    print("[POLLER] Thread started (interval=30s)")
    while True:
        try:
            client = get_client()
            conn = get_db()
            try:
                poll_once(client, conn)
            finally:
                conn.close()
        except Exception as e:
            print(f"[POLLER] Loop error: {e}")
        time.sleep(POLL_INTERVAL)


def start_poller():
    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()
    print("[POLLER] Started background MinIO poller")
