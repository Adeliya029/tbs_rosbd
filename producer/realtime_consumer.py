"""
TBS_ROSBD - Kafka Consumer for Realtime Monitoring
Flow: Kafka Topic → Consumer (untuk monitoring manual / testing)
- Consume earthquake-events dan system-logs secara paralel (threading)
- Graceful shutdown dengan signal handler
- Logging ke file dan terminal
"""

import os
import sys
import json
import time
import signal
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# Setup logging
LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "consumer.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger("rosbd_consumer")

# Konfigurasi
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", 
    "localhost:9092"
)

KAFKA_TOPIC_EARTHQUAKE = os.environ.get(
    "KAFKA_TOPIC_EARTHQUAKE", 
    "earthquake-events"
)

KAFKA_TOPIC_LOGS = os.environ.get(
    "KAFKA_TOPIC_LOGS", 
    "system-logs"
)

GROUP_ID = os.environ.get(
    "CONSUMER_GROUP_ID",
    "rosbd-consumer-group"
)

# Global flag untuk graceful shutdown
shutdown_event = threading.Event()


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Signal {signum} diterima, memulai shutdown...")
    shutdown_event.set()


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class EarthquakeConsumer:
    """Consumer untuk topic earthquake-events.

    Flow: Kafka Topic earthquake-events → Consumer (monitoring/testing)
    Note: Spark Structured Streaming akan consume topic yang sama secara terpisah.
    """

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = None
        self.messages_processed = 0
        self.running = False

    def connect(self) -> bool:
        """Buat koneksi ke Kafka."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                group_id=f"{self.group_id}-earthquake",
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=100,
                max_poll_interval_ms=300000
            )
            logger.info(f"Consumer earthquake-events tersambung ke {self.bootstrap_servers}")
            return True

        except NoBrokersAvailable:
            logger.error(f"Tidak ada broker Kafka di {self.bootstrap_servers}")
            return False
        except Exception as e:
            logger.error(f"Gagal konek consumer earthquake: {e}")
            return False

    def process_message(self, message) -> None:
        """Proses satu message earthquake."""
        data = message.value

        # Log ke terminal dengan format yang rapi
        print("\n" + "=" * 60)
        print("EARTHQUAKE EVENT")
        print("=" * 60)
        print(f"  Topic:     {message.topic}")
        print(f"  Partition: {message.partition}")
        print(f"  Offset:    {message.offset}")
        print(f"  Key:       {message.key}")
        print(f"  Timestamp: {datetime.fromtimestamp(message.timestamp / 1000).isoformat()}")
        print("-" * 60)
        print(f"  Event ID:     {data.get('event_id', 'N/A')}")
        print(f"  Waktu:        {data.get('datetime', 'N/A')}")
        print(f"  Magnitude:    {data.get('magnitude', 'N/A')}")
        print(f"  Lokasi:       {data.get('latitude', 'N/A')}, {data.get('longitude', 'N/A')}")
        print(f"  Kedalaman:    {data.get('depth', 'N/A')} km")
        print(f"  Wilayah:      {data.get('region', 'N/A')}")
        print(f"  Dirasakan:    {data.get('felt_intensity', 'N/A')}")
        print(f"  Source:       {data.get('source', 'N/A')}")
        print(f"  Ingested:     {data.get('ingested_at', 'N/A')}")
        print("=" * 60)

        # Log ke file logger
        logger.info(
            f"Processed earthquake | id={data.get('event_id', 'N/A')[:8]}... | "
            f"mag={data.get('magnitude')} | "
            f"lat={data.get('latitude')} lon={data.get('longitude')} | "
            f"depth={data.get('depth')}km | "
            f"region={data.get('region', 'N/A')[:30]} | "
            f"offset={message.offset}"
        )

        self.messages_processed += 1

    def run(self) -> None:
        """Main loop consumer."""
        if not self.connect():
            return

        self.running = True
        logger.info(f"Consumer earthquake-events mulai listening...")

        try:
            while not shutdown_event.is_set():
                # Poll dengan timeout agar bisa cek shutdown flag
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for message in records:
                        if shutdown_event.is_set():
                            break
                        self.process_message(message)

                if shutdown_event.is_set():
                    break

        except KafkaError as e:
            logger.error(f"Kafka error di consumer earthquake: {e}")
        except Exception as e:
            logger.error(f"Error di consumer earthquake: {e}")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop consumer gracefully."""
        self.running = False
        if self.consumer:
            logger.info(f"Consumer earthquake-events shutdown. Total processed: {self.messages_processed}")
            self.consumer.close(autocommit=True)


class SystemLogConsumer:
    """Consumer untuk topic system-logs.

    Flow: Kafka Topic system-logs → Consumer (monitoring/testing)
    Note: Streamlit System Monitoring page akan consume topic yang sama secara terpisah.
    """

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = None
        self.messages_processed = 0
        self.running = False

    def connect(self) -> bool:
        """Buat koneksi ke Kafka."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                group_id=f"{self.group_id}-logs",
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=100
            )
            logger.info(f"Consumer system-logs tersambung ke {self.bootstrap_servers}")
            return True

        except NoBrokersAvailable:
            logger.error(f"Tidak ada broker Kafka di {self.bootstrap_servers}")
            return False
        except Exception as e:
            logger.error(f"Gagal konek consumer logs: {e}")
            return False

    def process_message(self, message) -> None:
        """Proses satu message log."""
        data = message.value

        level = data.get('level', 'INFO')
        level_emoji = {
            'INFO': '[i]',
            'WARNING': '[!]',
            'ERROR': '[X]',
            'CRITICAL': '[!!]'
        }.get(level, '[i]')

        print(f"\n[{level_emoji} SYSTEM LOG - {level}]")
        print(f"  Topic:     {message.topic}")
        print(f"  Partition: {message.partition}")
        print(f"  Offset:    {message.offset}")
        print(f"  Timestamp: {data.get('timestamp', 'N/A')}")
        print(f"  Service:   {data.get('service', 'N/A')}")
        print(f"  Type:      {data.get('log_type', 'N/A')}")
        print(f"  Message:   {data.get('message', 'N/A')}")

        logger.info(
            f"Processed log | level={level} | service={data.get('service')} | "
            f"type={data.get('log_type')} | msg={data.get('message', 'N/A')[:50]} | "
            f"offset={message.offset}"
        )

        self.messages_processed += 1

    def run(self) -> None:
        """Main loop consumer."""
        if not self.connect():
            return

        self.running = True
        logger.info(f"Consumer system-logs mulai listening...")

        try:
            while not shutdown_event.is_set():
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for message in records:
                        if shutdown_event.is_set():
                            break
                        self.process_message(message)

                if shutdown_event.is_set():
                    break

        except KafkaError as e:
            logger.error(f"Kafka error di consumer logs: {e}")
        except Exception as e:
            logger.error(f"Error di consumer logs: {e}")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop consumer gracefully."""
        self.running = False
        if self.consumer:
            logger.info(f"Consumer system-logs shutdown. Total processed: {self.messages_processed}")
            self.consumer.close(autocommit=True)


def main():
    """Main function - jalankan kedua consumer dengan threading."""
    print("=" * 60)
    print("TBS_ROSBD - Kafka Consumer")
    print("=" * 60)
    print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Group ID:        {GROUP_ID}")
    print(f"Topics:          {KAFKA_TOPIC_EARTHQUAKE}, {KAFKA_TOPIC_LOGS}")
    print("-" * 60)
    print("Tekan Ctrl+C untuk berhenti.")
    print("=" * 60)

    # Buat consumer instances
    earthquake_consumer = EarthquakeConsumer(
        KAFKA_TOPIC_EARTHQUAKE,
        KAFKA_BOOTSTRAP_SERVERS,
        GROUP_ID
    )

    logs_consumer = SystemLogConsumer(
        KAFKA_TOPIC_LOGS,
        KAFKA_BOOTSTRAP_SERVERS,
        GROUP_ID
    )

    # Jalankan dengan threading
    thread_earthquake = threading.Thread(target=earthquake_consumer.run, name="EarthquakeConsumer")
    thread_logs = threading.Thread(target=logs_consumer.run, name="LogConsumer")

    thread_earthquake.start()
    thread_logs.start()

    logger.info("Kedua consumer thread telah dimulai")

    try:
        # Tunggu thread selesai
        thread_earthquake.join()
        thread_logs.join()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt diterima di main thread")
    finally:
        # Pastikan shutdown flag diset
        shutdown_event.set()

        # Tunggu thread selesai dengan timeout
        thread_earthquake.join(timeout=5)
        thread_logs.join(timeout=5)

        print("\n" + "=" * 60)
        print("Consumer ditutup.")
        print(f"  Earthquake events processed: {earthquake_consumer.messages_processed}")
        print(f"  System logs processed:       {logs_consumer.messages_processed}")
        print("=" * 60)


if __name__ == "__main__":
    main()