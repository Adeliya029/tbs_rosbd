# TBS_ROSBD - Pipeline Monitoring & Prediksi Gempa Indonesia

## 🚀 **SETUP CEPAT (SATU COMMAND)**

```bash
# 1. Pastikan Docker running
# 2. Aktifkan virtual environment
venv\Scripts\activate

# 3. Jalankan setup sekali
python setup_pipeline.py
```

## 📋 **APA YANG DISETUP OTOMATIS**

✅ **7 Container Docker:**
- Kafka + Zookeeper (localhost:9092)
- Spark Master + Worker (http://localhost:8080)
- MinIO (http://localhost:9001)
- PostgreSQL + PostGIS (seismic_db)
- Prefect (http://localhost:4200)

✅ **Kafka Topics:**
- `earthquake-events` (data gempa)
- `system-logs` (log sistem)

✅ **MinIO Buckets (6 bucket):**
- `historical-earthquake` (data historis)
- `raw-earthquake` (data streaming)
- `processed-features` (features ML)
- `trained-models` (model ML)
- `spark-checkpoints` (checkpoint Spark)
- `analytics` (data analytics)

✅ **PostgreSQL Database:**
- Database: `seismic_db`
- User: `rosbd` / Password: `rosbd123`
- Extension: PostGIS aktif
- 4 tabel siap: `earthquakes`, `earthquake_features`, `predictions`, `system_logs`

✅ **Data Awal:**
- 30 records data historis BMKG di MinIO
- Producer & Consumer Kafka siap testing

## 🔧 **FILE UTAMA**

### **Realtime Pipeline (FIXED):**
- `producer/realtime_producer.py` - Producer utama ✅
- `producer/realtime_consumer.py` - Consumer utama ✅
- `producer/test_producer.py` - Producer testing

### **Historical Pipeline (FIXED):**
- `batch/historical_web_scraper.py` - Web scraper utama ✅
- `batch/historical_scraper_final.py` - API scraper alternatif
- `batch/simple_historical_scrapper.py` - Scraper API BMKG

### **Spark Jobs (siap jalan):**
- `spark/jobs/kafka_to_minio.py` - Streaming: Kafka → MinIO
- `spark/jobs/minio_to_postgres.py` - Batch: MinIO → PostgreSQL

### **Setup Scripts:**
- `setup_pipeline.py` - Setup semua (satu command) ✨
- `setup_minio_buckets.py` - Setup bucket MinIO
- `setup_postgres.sql` - Setup database PostgreSQL

## 🧪 **TEST SETUP**

```bash
# Test 1: Producer → Kafka
python producer/test_producer.py

# Test 2: Consumer ← Kafka
python producer/simple_kafka_consumer.py

# Test 3: Cek MinIO
python -c "from minio import Minio; client=Minio('localhost:9000','admin','admin12345',False); print([obj.object_name for obj in client.list_objects('historical-earthquake')])"
```

## 🌐 **ACCESS POINTS**

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO | http://localhost:9001 | admin / admin12345 |
| Spark UI | http://localhost:8080 | - |
| Prefect | http://localhost:4200 | - |
| Kafka | localhost:9092 | - |
| PostgreSQL | rosbd_postgres:5432 | rosbd / rosbd123 |

## 🚨 **TROUBLESHOOTING**

### **Jika Kafka error:**
```bash
docker compose restart kafka
docker exec rosbd_kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### **Jika MinIO error:**
```bash
docker compose restart minio
python setup_minio_buckets.py
```

### **Jika PostgreSQL error:**
```bash
docker compose restart rosbd_postgres
docker exec rosbd_postgres psql -U rosbd -d seismic_db -c "SELECT version();"
```

## 📞 **CREDENTIALS**

- **MinIO:** admin / admin12345
- **PostgreSQL:** rosbd / rosbd123
- **Database:** seismic_db
- **Kafka:** localhost:9092

## 🎯 **STATUS PIPELINE**

```
✅ BMKG API → Producer → Kafka → Consumer
✅ BMKG API → Historical Scraper → MinIO
✅ Docker Infrastructure → Running
✅ Spark Cluster → Active  
✅ PostgreSQL → Ready with PostGIS
⬜ Kafka → [Spark Streaming] → MinIO (siap jalan)
⬜ MinIO → [Spark Batch] → PostgreSQL (siap jalan)
⬜ [ML Training] → Models → [FastAPI] (next phase)
⬜ [Dashboard] ← PostgreSQL (next phase)
```

**Pipeline dasar sudah berfungsi dan siap untuk dikembangkan!** 🚀