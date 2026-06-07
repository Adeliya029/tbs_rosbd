"""
TBS_ROSBD - Realtime Kafka Consumer
Membaca data dari topic earthquake-events dan system-logs.
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("rosbd_consumer")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_EARTHQUAKE = "earthquake-events"
KAFKA_TOPIC_LOGS = "system-logs"

class RealtimeConsumer:
    """Consumer untuk membaca data realtime dari Kafka."""
    
    def __init__(self):
        self.consumer_events = None
        self.consumer_logs = None
        self.koneksi_kafka()
    
    def koneksi_kafka(self):
        """Membuat koneksi ke Kafka untuk kedua topic."""
        try:
            # Consumer untuk earthquake-events
            self.consumer_events = KafkaConsumer(
                KAFKA_TOPIC_EARTHQUAKE,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='rosbd_consumer_group'
            )
            
            # Consumer untuk system-logs
            self.consumer_logs = KafkaConsumer(
                KAFKA_TOPIC_LOGS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='rosbd_logs_group'
            )
            
            logger.info(f"Berhasil konek ke Kafka di {KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"Topic: {KAFKA_TOPIC_EARTHQUAKE}, {KAFKA_TOPIC_LOGS}")
            
        except Exception as e:
            logger.error(f"Gagal konek ke Kafka: {e}")
            raise
    
    def proses_event_gempa(self, message):
        """Proses message dari topic earthquake-events."""
        data = message.value
        
        print("\n" + "=" * 60)
        print("🌍 EARTHQUAKE EVENT")
        print("=" * 60)
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key.decode() if message.key else 'N/A'}")
        print("-" * 60)
        print(f"Event ID: {data.get('event_id')}")
        print(f"Waktu: {data.get('datetime')}")
        print(f"Lokasi: {data.get('latitude')}, {data.get('longitude')}")
        print(f"Magnitude: {data.get('magnitude')}")
        print(f"Kedalaman: {data.get('depth')} km")
        print(f"Wilayah: {data.get('region')}")
        print(f"Sumber: {data.get('source')}")
        print(f"Waktu ingest: {data.get('ingested_at')}")
        print("=" * 60)
    
    def proses_log_system(self, message):
        """Proses message dari topic system-logs."""
        data = message.value
        
        level = data.get('level', 'INFO')
        level_icon = {
            'INFO': '📝',
            'ERROR': '❌',
            'WARNING': '⚠️',
            'SUCCESS': '✅'
        }.get(level, '📝')
        
        print(f"\n{level_icon} SYSTEM LOG [{level}]")
        print(f"  Service: {data.get('service')}")
        print(f"  Type: {data.get('log_type')}")
        print(f"  Time: {data.get('timestamp')}")
        print(f"  Message: {data.get('message')}")
    
    def jalankan(self):
        """Loop utama untuk membaca dari kedua topic."""
        logger.info("Consumer TBS_ROSBD mulai. Menunggu data...")
        
        print("\n" + "=" * 60)
        print("TBS_ROSBD REALTIME CONSUMER")
        print("=" * 60)
        print("Mendengarkan:")
        print(f"  1. {KAFKA_TOPIC_EARTHQUAKE} - Data gempa realtime")
        print(f"  2. {KAFKA_TOPIC_LOGS} - Log sistem")
        print("=" * 60)
        print("Tekan Ctrl+C untuk berhenti.")
        print("-" * 60)
        
        try:
            while True:
                # Poll earthquake events
                event_batch = self.consumer_events.poll(timeout_ms=1000)
                for tp, messages in event_batch.items():
                    for message in messages:
                        self.proses_event_gempa(message)
                
                # Poll system logs
                log_batch = self.consumer_logs.poll(timeout_ms=1000)
                for tp, messages in log_batch.items():
                    for message in messages:
                        self.proses_log_system(message)
                
        except KeyboardInterrupt:
            logger.info("Consumer dihentikan oleh user")
        
        finally:
            if self.consumer_events:
                self.consumer_events.close()
            if self.consumer_logs:
                self.consumer_logs.close()
            logger.info("Kafka consumer ditutup")

if __name__ == "__main__":
    consumer = RealtimeConsumer()
    consumer.jalankan()