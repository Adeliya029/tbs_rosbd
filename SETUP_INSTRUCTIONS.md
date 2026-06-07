# SETUP INSTRUCTIONS - TBS_ROSBD

## 🚀 **QUICK START**

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

## 📋 **ENVIRONMENT VARIABLES (.env)**

### **Default Values for Local Development:**
```env
# POSTGRES
POSTGRES_USER=rosbd
POSTGRES_PASSWORD=rosbd123
POSTGRES_DB=seismic_db

# MINIO  
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin12345
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=admin12345
MINIO_SECURE=false

# KAFKA
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_EARTHQUAKE=earthquake-events
KAFKA_TOPIC_LOGS=system-logs

# BMKG
BMKG_API_URL=https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json
POLLING_INTERVAL=15

# LOG
LOG_DIR=logs
```

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
│
└── setup_scripts/         # Setup utilities
    ├── setup_minio_buckets.py
    ├── setup_postgres.sql
    └── QUICKSTART.md
```

## 🌐 **ACCESS POINTS**

| Service | URL | Default Credentials |
|---------|-----|-------------------|
| MinIO Console | http://localhost:9001 | admin / admin12345 |
| Spark Master UI | http://localhost:8080 | - |
| Prefect UI | http://localhost:4200 | - |

## 🔧 **TROUBLESHOOTING**

### **If Docker containers fail:**
```bash
# Check logs
docker compose logs

# Restart containers
docker compose restart

# Rebuild if needed
docker compose up -d --build
```

### **If Python packages missing:**
```bash
# Reinstall requirements
pip install -r requirements.txt

# Or install specific package
pip install kafka-python minio pandas requests
```

### **If Kafka not working:**
```bash
# Check Kafka status
docker exec rosbd_kafka kafka-topics --list --bootstrap-server localhost:9092

# Restart Kafka
docker compose restart kafka
```

## 📞 **SUPPORT**

### **Default Credentials:**
- **MinIO:** admin / admin12345
- **PostgreSQL:** rosbd / rosbd123
- **Database:** seismic_db

### **Ports:**
- Kafka: 9092
- MinIO API: 9000, Console: 9001
- Spark Master: 8080
- Spark Worker: 8081
- Prefect: 4200
- PostgreSQL: 5432 (internal)

## 🎉 **NEXT STEPS**

After setup:
1. Test producer and consumer
2. Run historical scraper for data
3. Start Spark jobs for processing
4. Develop ML models and dashboard

**Happy coding!** 🚀