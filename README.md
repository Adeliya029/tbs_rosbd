# TBS_ROSBD - Pipeline Monitoring & Prediksi Gempa Indonesia

## 🚀 **SETUP CEPAT (SATU COMMAND)**

### **1. Clone Repository**
```bash
git clone <repository-url>
cd tbs_rosbd_project
```

### **2. Setup Environment**
```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your values
# (Use default values for local development)
```

### **3. Install Dependencies**
```bash
# Create virtual environment (optional but recommended)
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install packages
pip install -r requirements.txt
```

### **4. Start Infrastructure**
```bash
# Start all Docker containers
docker compose up -d

# Wait for containers to be ready (30 seconds)
# Or run the setup script:
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

## 🧪 **TEST PIPELINE**

### **Test 1: Producer**
```bash
python producer/test_producer.py
```

### **Test 2: Consumer**
```bash
python producer/realtime_consumer.py
```

### **Test 3: Historical Data**
```bash
python batch/historical_web_scraper.py
```

## 🎯 **FILE STRUCTURE**

### **Core Files:**
```
📁 tbs_rosbd_project/
├── .env.example              # Environment template
├── .gitignore               # Git ignore rules
├── README.md                # Main documentation
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Docker containers
├── setup_pipeline.py        # Auto setup script
│
├── producer/               # Realtime pipeline
│   ├── realtime_producer.py    # Main producer
│   ├── realtime_consumer.py    # Main consumer
│   └── test_producer.py        # Test producer
│
├── batch/                  # Historical data
│   └── historical_web_scraper.py  # Web scraper
│
├── spark/jobs/            # Spark processing
│   ├── kafka_to_minio.py     # Streaming job
│   └── minio_to_postgres.py  # Batch job

```
## 🌐 **ACCESS POINTS**

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO | http://localhost:9001 | admin / admin12345 |
| Spark UI | http://localhost:8080 | - |
| Prefect | http://localhost:4200 | - |
| Kafka | localhost:9092 | - |
| PostgreSQL | rosbd_postgres:5432 | rosbd / rosbd123 |

```

## 📞 **CREDENTIALS**

- **MinIO:** admin / admin12345
- **PostgreSQL:** rosbd / rosbd123
- **Database:** seismic_db
- **Kafka:** localhost:9092

