"""Shared DB helpers for saving predictions & anomalies."""

import psycopg2
import api.config as cfg


def get_db():
    return psycopg2.connect(
        host=cfg.PG_HOST,
        port=cfg.PG_PORT,
        dbname=cfg.PG_DATABASE,
        user=cfg.PG_USER,
        password=cfg.PG_PASSWORD,
    )


def save_prediction(conn, features, result):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO risk_scores
                (region, latitude, longitude, magnitude, depth_km,
                 quake_count_24h, avg_magnitude_24h, max_magnitude_24h,
                 shallow_quake_ratio_24h, felt_count_24h, swarm_density,
                 risk_class, risk_probability, anomaly_score, event_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                features["region"], features["latitude"], features["longitude"],
                features["magnitude"], features["depth_km"],
                int(features["quake_count_24h"]), features["avg_magnitude_24h"],
                features["max_magnitude_24h"], features["shallow_quake_ratio_24h"],
                int(features["felt_count_24h"]), features["swarm_density"],
                result["risk_class"], result["risk_probability"],
                result["anomaly_score"], features["event_time"],
            ),
        )
        conn.commit()


def save_anomaly(conn, features, result):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomaly_events
                (anomaly_score, region, latitude, longitude, magnitude, event_time)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                result["anomaly_score"], features["region"],
                features["latitude"], features["longitude"],
                features["magnitude"], features["event_time"],
            ),
        )
        conn.commit()
