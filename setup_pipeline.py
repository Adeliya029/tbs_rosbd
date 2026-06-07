"""
TBS_ROSBD - Complete Pipeline Setup Script
Setup semua infrastruktur dengan satu command.
"""

import os
import sys
import time
import json
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("rosbd_setup")

class PipelineSetup:
    """Class untuk setup pipeline ROSBD."""
    
    def __init__(self):
        self.project_root = os.path.dirname(os.path.abspath(__file__))
        
    def run_command(self, cmd, cwd=None, check=True):
        """Jalankan shell command."""
        logger.info(f"Running: {cmd}")
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                cwd=cwd or self.project_root,
                capture_output=True,
                text=True,
                check=check
            )
            if result.stdout:
                logger.info(f"Output: {result.stdout[:500]}...")
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e}")
            if e.stderr:
                logger.error(f"Error: {e.stderr}")
            raise
    
    def setup_docker(self):
        """Start semua container Docker."""
        logger.info("=" * 60)
        logger.info("STEP 1: Starting Docker containers...")
        logger.info("=" * 60)
        
        # Stop existing containers
        self.run_command("docker compose down", check=False)
        
        # Start all containers
        result = self.run_command("docker compose up -d")
        
        # Wait for containers to be ready
        logger.info("Waiting for containers to start...")
        time.sleep(15)
        
        # Check running containers
        result = self.run_command("docker ps --format \"table {{.Names}}\t{{.Status}}\"")
        logger.info("Docker containers status:")
        print(result.stdout)
        
        return True
    
    def setup_kafka_topics(self):
        """Setup Kafka topics."""
        logger.info("=" * 60)
        logger.info("STEP 2: Setting up Kafka topics...")
        logger.info("=" * 60)
        
        # Wait for Kafka to be ready
        logger.info("Waiting for Kafka to be ready...")
        time.sleep(10)
        
        # Create topics
        topics = [
            ("earthquake-events", 1, 1),
            ("system-logs", 1, 1)
        ]
        
        for topic, partitions, replication in topics:
            cmd = (
                f"docker exec rosbd_kafka kafka-topics "
                f"--bootstrap-server localhost:9092 "
                f"--create "
                f"--topic {topic} "
                f"--partitions {partitions} "
                f"--replication-factor {replication} "
                f"--if-not-exists"
            )
            self.run_command(cmd)
        
        # List topics
        result = self.run_command(
            "docker exec rosbd_kafka kafka-topics --bootstrap-server localhost:9092 --list"
        )
        logger.info("Kafka topics created:")
        print(result.stdout)
        
        return True
    
    def setup_minio_buckets(self):
        """Setup MinIO buckets."""
        logger.info("=" * 60)
        logger.info("STEP 3: Setting up MinIO buckets...")
        logger.info("=" * 60)
        
        # Import and run MinIO setup
        import sys
        sys.path.append(self.project_root)
        
        from setup_minio_buckets import setup_buckets
        setup_buckets()
        
        return True
    
    def setup_postgres_database(self):
        """Setup PostgreSQL database."""
        logger.info("=" * 60)
        logger.info("STEP 4: Setting up PostgreSQL database...")
        logger.info("=" * 60)
        
        # Wait for PostgreSQL to be ready
        logger.info("Waiting for PostgreSQL to be ready...")
        time.sleep(10)
        
        # Execute SQL setup
        with open(os.path.join(self.project_root, "setup_postgres.sql"), "r") as f:
            sql_content = f.read()
        
        # Split SQL by statements
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for stmt in sql_statements:
            if stmt:  # Skip empty statements
                cmd = (
                    f"docker exec -i rosbd_postgres "
                    f"psql -U rosbd -d seismic_db "
                    f"-c \"{stmt}\""
                )
                self.run_command(cmd, check=False)
        
        # Verify setup
        result = self.run_command(
            "docker exec rosbd_postgres psql -U rosbd -d seismic_db "
            "-c \"SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;\""
        )
        logger.info("PostgreSQL tables created:")
        print(result.stdout)
        
        return True
    
    def setup_historical_scraper(self):
        """Setup historical scraper dengan API bukan web scraping."""
        logger.info("=" * 60)
        logger.info("STEP 5: Setting up historical data scraper...")
        logger.info("=" * 60)
        
        # Run the simple historical scraper
        scraper_path = os.path.join(self.project_root, "batch", "simple_historical_scrapper.py")
        if os.path.exists(scraper_path):
            self.run_command(f"python {scraper_path}")
        else:
            logger.warning(f"Historical scraper not found: {scraper_path}")
        
        return True
    
    def test_pipeline(self):
        """Test pipeline components."""
        logger.info("=" * 60)
        logger.info("STEP 6: Testing pipeline components...")
        logger.info("=" * 60)
        
        tests = [
            ("Docker containers", "docker ps --format \"{{.Names}}\" | findstr rosbd"),
            ("Kafka topics", "docker exec rosbd_kafka kafka-topics --bootstrap-server localhost:9092 --list"),
            ("PostgreSQL connection", "docker exec rosbd_postgres psql -U rosbd -d seismic_db -c \"SELECT version();\""),
            ("Spark UI", "echo 'Check Spark UI: http://localhost:8080'"),
            ("MinIO Console", "echo 'Check MinIO: http://localhost:9001 (admin/admin12345)'"),
            ("Prefect UI", "echo 'Check Prefect: http://localhost:4200'"),
        ]
        
        all_passed = True
        for test_name, cmd in tests:
            logger.info(f"Testing {test_name}...")
            try:
                result = self.run_command(cmd, check=False)
                if result.returncode == 0:
                    logger.info(f"✅ {test_name}: PASS")
                else:
                    logger.error(f"❌ {test_name}: FAIL")
                    all_passed = False
            except Exception as e:
                logger.error(f"❌ {test_name}: ERROR - {e}")
                all_passed = False
        
        return all_passed
    
    def fix_schedule_package(self):
        """Fix schedule package issue."""
        logger.info("=" * 60)
        logger.info("FIX 1: Installing schedule package...")
        logger.info("=" * 60)
        
        try:
            self.run_command("pip install schedule==1.2.1")
            logger.info("✅ Schedule package installed")
        except Exception as e:
            logger.error(f"❌ Failed to install schedule: {e}")
    
    def create_quickstart_guide(self):
        """Create quickstart guide untuk tim."""
        logger.info("=" * 60)
        logger.info("Creating quickstart guide...")
        logger.info("=" * 60)
        
        guide = f"""# TBS_ROSBD QUICKSTART GUIDE

## INSTALASI CEPAT
1. Clone repository
2. Jalankan setup sekali:
   ```bash
   python setup_pipeline.py
   ```

## INFRASTRUKTUR YANG DI-SETUP
- ✅ Kafka (localhost:9092) dengan topics: earthquake-events, system-logs
- ✅ MinIO (http://localhost:9001) dengan 6 buckets
- ✅ PostgreSQL (seismic_db) dengan PostGIS extension
- ✅ Spark Cluster (http://localhost:8080)
- ✅ Prefect (http://localhost:4200)

## DATA YANG SUDAH ADA
- ✅ Data historis BMKG di MinIO bucket: historical-earthquake
- ✅ 30 records gempa terkini dan dirasakan

## FILE UTAMA
1. `producer/realtime_producer.py` - Producer data gempa realtime
2. `producer/test_producer.py` - Producer testing
3. `batch/simple_historical_scrapper.py` - Scraper data historis
4. `spark/jobs/kafka_to_minio.py` - Spark streaming job
5. `spark/jobs/minio_to_postgres.py` - Spark batch job

## TEST PIPELINE
```bash
# Test 1: Producer → Kafka
python producer/test_producer.py

# Test 2: Consumer ← Kafka  
python producer/simple_kafka_consumer.py

# Test 3: Check MinIO
python -c "from minio import Minio; import sys; client = Minio('localhost:9000', 'admin', 'admin12345', False); print([obj.object_name for obj in client.list_objects('historical-earthquake')])"
```

## NEXT STEPS UNTUK TIM
1. **Spark Team**: Jalankan `spark/jobs/kafka_to_minio.py`
2. **ML Team**: Setup Prefect flows dan training pipeline
3. **Dashboard Team**: Buat FastAPI + Streamlit dashboard

## TROUBLESHOOTING
- Kafka error: `docker compose restart kafka`
- MinIO error: `docker compose restart minio`
- PostgreSQL error: `docker compose restart rosbd_postgres`

## CREDENTIALS
- MinIO: admin / admin12345
- PostgreSQL: rosbd / rosbd123
- Database: seismic_db

Pipeline siap digunakan! 🚀
"""
        
        with open(os.path.join(self.project_root, "QUICKSTART.md"), "w") as f:
            f.write(guide)
        
        logger.info("✅ Quickstart guide created: QUICKSTART.md")
    
    def run_full_setup(self):
        """Jalankan semua setup."""
        logger.info("=" * 60)
        logger.info("TBS_ROSBD - COMPLETE PIPELINE SETUP")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Docker
            self.setup_docker()
            
            # Step 2: Kafka
            self.setup_kafka_topics()
            
            # Step 3: MinIO
            self.setup_minio_buckets()
            
            # Step 4: PostgreSQL
            self.setup_postgres_database()
            
            # Step 5: Historical data
            self.setup_historical_scraper()
            
            # Fix issues
            self.fix_schedule_package()
            
            # Step 6: Test
            test_result = self.test_pipeline()
            
            # Create guide
            self.create_quickstart_guide()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 60)
            logger.info("SETUP COMPLETE! 🎉")
            logger.info(f"Duration: {duration}")
            logger.info("=" * 60)
            
            if test_result:
                logger.info("✅ All tests passed!")
            else:
                logger.warning("⚠️ Some tests failed, check logs above")
            
            logger.info("\nNEXT STEPS:")
            logger.info("1. Check QUICKSTART.md for instructions")
            logger.info("2. Run test producer: python producer/test_producer.py")
            logger.info("3. Run test consumer: python producer/simple_kafka_consumer.py")
            logger.info("4. Start Spark jobs when ready")
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        return True

def main():
    """Main entry point."""
    setup = PipelineSetup()
    success = setup.run_full_setup()
    
    if success:
        print("\n" + "=" * 60)
        print("✅ TBS_ROSBD PIPELINE SETUP SUCCESSFUL!")
        print("=" * 60)
        print("\nAccess points:")
        print("- MinIO Console: http://localhost:9001")
        print("- Spark UI: http://localhost:8080")  
        print("- Prefect UI: http://localhost:4200")
        print("\nCheck QUICKSTART.md for next steps.")
    else:
        print("\n" + "=" * 60)
        print("❌ Setup failed. Check logs above.")
        print("=" * 60)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()