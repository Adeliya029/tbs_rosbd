# Setup Guide — Prefect 3 Laptop
## Real-Time Seismic Intelligence System · ROSBD REV 3.0

```
Laptop 1  →  Prefect Worker (scraping-pool) + Kafka
Laptop 2  →  Prefect Worker (training-pool) + MinIO + Spark Pipeline
Laptop 3  →  Prefect Server + PostgreSQL + FastAPI + Streamlit
```

Semua koneksi via **Tailscale** (bukan LAN).

| Laptop | Nama | Tailscale IP | Service |
|--------|------|-------------|---------|
| Laptop 1 | adel | `100.76.33.80` | Kafka (:9093), Scraping Worker |
| Laptop 2 | najma | `100.122.2.11` | MinIO (:9000), Spark, Training Worker |
| Laptop 3 | auliaa-cantik | `100.71.251.60` | Prefect Server (:4200), PostgreSQL (:5432), FastAPI (:8000) |

---

## Prasyarat — Semua Laptop

- Docker Desktop terinstall dan running
- Python 3.11+ terinstall
- Tailscale terinstall dan login ke akun `komterkelompok2`
- Tailscale status semua laptop **active**

---

## Step 1 — Setup Laptop 3 (Server) — DULUAN

Laptop 3 host Prefect Server + PostgreSQL. Harus jalan duluan sebelum Laptop 1 & 2.

```bash
# 1. Masuk ke folder laptop3
cd deployment/laptop3

# 2. Copy dan edit .env
cp .env.example .env
# (default sudah benar, tidak perlu edit)

# 3. Jalankan semua service
docker compose up -d

# 4. Cek semua service jalan
docker compose ps

# 5. Verifikasi Prefect Server
curl http://localhost:4200/api/health
# Harusnya: {"status":"healthy"}

# 6. Verifikasi PostgreSQL
docker compose exec postgres psql -U rosbd -d prefect_db -c "SELECT 1"
```

### Verifikasi dari laptop lain (via Tailscale)

```bash
# Dari Laptop 1 atau 2:
curl http://100.71.251.60:4200/api/health
```

---

## Step 2 — Setup Laptop 1 (Worker Scraping)

```bash
# 1. Masuk ke folder laptop1
cd deployment/laptop1

# 2. Copy dan edit .env
cp .env.example .env

# 3. WAJIB: isi Tailscale IP Laptop 3
# Buka .env, pastikan:
# PREFECT_API_URL=http://100.71.251.60:4200/api

# 4. Jalankan worker + Kafka
docker compose up -d

# 5. Cek worker muncul di Prefect UI
# Buka browser: http://100.71.251.60:4200
# → Work Pools → scraping-pool → ada worker "laptop1-scraping"
```

### Register flows ke server (sekali saja)
```bash
docker compose --profile deploy run --rm prefect-deploy
```

---

## Step 3 — Setup Laptop 2 (Worker Training + Spark + MinIO)

### A. Setup Infrastruktur Dasar

```powershell
# Di PowerShell, dari root project:
.\deployment\laptop2\setup.ps1
```

Ini menjalankan MinIO + Spark Master + Spark Worker.

### B. Setup Prefect Worker

```bash
# 1. Masuk ke folder laptop2/prefect
cd deployment/laptop2/prefect

# 2. Copy dan edit .env
cp .env.example .env

# 3. WAJIB: isi Tailscale IP
# PREFECT_API_URL=http://100.71.251.60:4200/api
# MINIO_ENDPOINT=rosbd_minio:9000
# MINIO_ACCESS_KEY=admin
# MINIO_SECRET_KEY=admin12345

# 4. Jalankan worker
docker compose up -d

# 5. Cek worker muncul di Prefect UI
# Buka browser: http://100.71.251.60:4200
# → Work Pools → training-pool → ada worker "laptop2-training"
```

### C. Register flows ke server (sekali saja)
```bash
docker compose --profile deploy run --rm prefect-deploy
```

### D. Jalankan Spark Pipeline

```powershell
# Masuk container Spark
docker exec -it rosbd_spark_master bash

# Jalankan pipeline
KAFKA_BOOTSTRAP_SERVERS=100.76.33.80:9093 MINIO_ENDPOINT=rosbd_minio:9000 \
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  /opt/spark/jobs/spark_pipeline.py
```

---

## Arsitektur Lengkap

```
┌─ Laptop 1: 100.76.33.80 ──────────────────────────────────────┐
│                                                                 │
│  🏗️  scraping-pool (Prefect Worker)                            │
│      → Task: historical scraping, data collection               │
│                                                                 │
│  📨 Kafka :9093                                                 │
│      → Topic: earthquake-events                                 │
│                                                                 │
│  BMKG API ──→ producer ──→ Kafka                                │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Tailscale
                       ▼
┌─ Laptop 2: 100.122.2.11 ──────────────────────────────────────┐
│                                                                 │
│  🏋️  training-pool (Prefect Worker)                            │
│      → build_aggregate_dataset.py                               │
│      → train_isolation_forest.py                                │
│      → train_random_forest.py                                   │
│                                                                 │
│  ⚡ Spark Pipeline (rosbd_spark_master)                         │
│      → Kafka consumer → Feature Engineering → ST-DBSCAN         │
│      → Snapshot 24h → POST FastAPI Laptop 3                     │
│                                                                 │
│  📦 MinIO :9000                                                 │
│      raw-earthquake/         ← Kafka raw events (JSON)          │
│      processed-features/     ← Events + snapshots (Parquet)     │
│      trained-models/         ← RF + IF models (.pkl)            │
│      historical-earthquake/  ← BMKG historical CSV              │
│                                                                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Tailscale
                       ▼
┌─ Laptop 3: 100.71.251.60 ─────────────────────────────────────┐
│                                                                 │
│  👑 Prefect Server :4200                                        │
│     Work Pools: scraping-pool, training-pool                    │
│                                                                 │
│  🤖 FastAPI :8000                                               │
│     POST /api/predict → RF + IF inference                       │
│                                                                 │
│  🗄️  PostgreSQL :5432                                           │
│     seismic_db    → data gempa + prediksi                       │
│     prefect_db    → Prefect metadata                            │
│                                                                 │
│  📊 Streamlit :8501                                             │
│     Dashboard monitoring                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Urutan Startup

```
1. Laptop 3 up  →  Prefect Server + PostgreSQL
2. Laptop 1 up  →  Kafka + Scraping Worker
3. Laptop 2 up  →  MinIO + Spark + Training Worker
4. Laptop 1     →  Jalankan producer (realtime kirim ke Kafka)
5. Laptop 2     →  Jalankan Spark Pipeline (Kafka → MinIO → FastAPI)
6. Laptop 2     →  Training: manual pertama kali, lalu auto via Prefect
```

---

## Jadwal Training (Prefect)

| Flow | Jadwal | Work Pool |
|------|--------|-----------|
| `build-training-dataset` | Mingguan, Senin 01:00 WIB | training-pool |
| `train-isolation-forest` | Mingguan, Senin 02:00 WIB | training-pool |
| `train-random-forest` | Mingguan, Senin 02:30 WIB | training-pool |

Bisa juga trigger manual dari Prefect UI kapan saja.

---

## Monitoring

| Service | URL | Laptop |
|---------|-----|--------|
| Prefect UI | `http://100.71.251.60:4200` | Laptop 3 |
| MinIO Console | `http://localhost:9001` | Laptop 2 |
| Spark Master UI | `http://localhost:8080` | Laptop 2 |
| FastAPI Docs | `http://100.71.251.60:8000/docs` | Laptop 3 |
| Streamlit Dashboard | `http://100.71.251.60:8501` | Laptop 3 |

---

## Credentials

| Service | Username | Password |
|---------|----------|----------|
| MinIO | `admin` | `admin12345` |
| PostgreSQL | `rosbd` | `rosbd123` |

---

## Troubleshooting

### Worker tidak muncul di Prefect UI
```bash
# Cek PREFECT_API_URL di worker
docker compose exec prefect-worker env | grep PREFECT

# Test koneksi ke server dari laptop 2
curl http://100.71.251.60:4200/api/health

# Cek log worker
docker compose logs prefect-worker --tail 50
```

### Spark streaming tidak bisa konek Kafka
```powershell
# Test dari container Spark ke Laptop 1
docker exec rosbd_spark_master bash -c "echo >/dev/tcp/100.76.33.80/9093 && echo OK || echo FAIL"

# Cek Kafka masih ada data
docker run --rm edenhill/kcat:1.7.1 -b 100.76.33.80:9093 -L
```

### Training gagal karena tidak bisa akses MinIO
```bash
# Worker training akses MinIO via Docker network
docker compose exec prefect-worker curl http://rosbd_minio:9000/minio/health/live

# Pastikan MINIO_ENDPOINT di .env = rosbd_minio:9000 (bukan localhost)
```

### IP Tailscale berubah
Tailscale IP **tidak berubah** setelah login — kecuali di-reinstall. Kalau berubah:
1. Update semua `.env` di 3 laptop
2. Restart worker: `docker compose restart`
3. Update `KAFKA_BOOTSTRAP_SERVERS` di `spark_pipeline.py`
