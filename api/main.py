import json
from datetime import datetime, timezone

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

from api.model_loader import load_models
from api.inference import predict_snapshot
from api.alert import trigger_alerts
from api.db import save_prediction, save_anomaly
from api.minio_poller import start_poller
import api.config as cfg

app = FastAPI(title="TBS-ROSBD Inference API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Database ──────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(
        host=cfg.PG_HOST,
        port=cfg.PG_PORT,
        dbname=cfg.PG_DATABASE,
        user=cfg.PG_USER,
        password=cfg.PG_PASSWORD,
    )
    try:
        yield conn
    finally:
        conn.close()


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS risk_scores (
                id SERIAL PRIMARY KEY,
                region VARCHAR(255),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                magnitude DOUBLE PRECISION,
                depth_km DOUBLE PRECISION,
                quake_count_24h INTEGER,
                avg_magnitude_24h DOUBLE PRECISION,
                max_magnitude_24h DOUBLE PRECISION,
                shallow_quake_ratio_24h DOUBLE PRECISION,
                felt_count_24h INTEGER,
                swarm_density DOUBLE PRECISION,
                risk_class VARCHAR(20),
                risk_probability DOUBLE PRECISION,
                anomaly_score DOUBLE PRECISION,
                event_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS anomaly_events (
                id SERIAL PRIMARY KEY,
                anomaly_score DOUBLE PRECISION,
                region VARCHAR(255),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                magnitude DOUBLE PRECISION,
                event_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS alert_history (
                id SERIAL PRIMARY KEY,
                alert_type VARCHAR(50),
                level VARCHAR(20),
                message TEXT,
                region VARCHAR(255),
                risk_class VARCHAR(20),
                anomaly_score DOUBLE PRECISION,
                swarm_density DOUBLE PRECISION,
                is_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Database tables ensured")


# ── Startup ───────────────────────────────────────────────

@app.on_event("startup")
def on_startup():
    print("=" * 50)
    print("TBS-ROSBD FastAPI Starting...")
    try:
        load_models()
    except Exception as e:
        print(f"[WARN] Gagal load model dari MinIO: {e}")
        print("[WARN] FastAPI tetap jalan, /api/predict akan return UNKNOWN sampai model tersedia")
    conn = psycopg2.connect(
        host=cfg.PG_HOST,
        port=cfg.PG_PORT,
        dbname=cfg.PG_DATABASE,
        user=cfg.PG_USER,
        password=cfg.PG_PASSWORD,
    )
    init_db(conn)
    conn.close()
    start_poller()
    print("=" * 50)


# ── Schemas ───────────────────────────────────────────────

class FeaturePayload(BaseModel):
    event_time: str
    region: str
    latitude: float
    longitude: float
    magnitude: float
    depth_km: float
    quake_count_24h: float
    avg_magnitude_24h: float
    max_magnitude_24h: float
    shallow_quake_ratio_24h: float
    felt_count_24h: float
    swarm_density: float


class InferenceResult(BaseModel):
    risk_class: str
    risk_probability: float
    anomaly_score: float


# ── Endpoints ─────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "TBS-ROSBD Inference API", "status": "running"}


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "models_loaded": True,
    }


@app.post("/api/predict", response_model=InferenceResult)
def predict(payload: FeaturePayload, conn=Depends(get_db)):
    features = payload.model_dump()

    result = predict_snapshot(features)

    result["region"] = payload.region
    result["event_time"] = payload.event_time
    result["latitude"] = payload.latitude
    result["longitude"] = payload.longitude
    result["magnitude"] = payload.magnitude
    result["depth_km"] = payload.depth_km
    result["swarm_density"] = payload.swarm_density

    save_prediction(conn, features, result)

    if result["anomaly_score"] > cfg.ANOMALY_WARNING_THRESHOLD:
        save_anomaly(conn, features, result)

    alert_messages = trigger_alerts({**features, **result})

    print(f"Predicted: region={payload.region} class={result['risk_class']} prob={result['risk_probability']:.3f} anomaly={result['anomaly_score']:.3f}")

    return InferenceResult(
        risk_class=result["risk_class"],
        risk_probability=result["risk_probability"],
        anomaly_score=result["anomaly_score"],
    )


@app.get("/api/risk-scores")
def get_risk_scores(limit: int = 100, conn=Depends(get_db)):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM risk_scores ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


@app.get("/api/anomaly")
def get_anomalies(limit: int = 100, conn=Depends(get_db)):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM anomaly_events ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


@app.get("/api/alerts")
def get_alerts(limit: int = 50, conn=Depends(get_db)):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM alert_history ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


# ── Helpers ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host=cfg.HOST, port=cfg.PORT, reload=True)
