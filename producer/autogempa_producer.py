"""
TBS_ROSBD - Producer Data Gempa BMKG (Autogempa)
Flow: BMKG Autogempa API -> Kafka Producer
- Sumber: https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json
- Polling setiap POLLING_INTERVAL detik
- Deduplication berdasarkan Shakemap ID (datetime unik)
- Output: [TERKIRIM] format
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "producer.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger("autogempa_producer")

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

BMKG_API_URL_AUTOGEMPA = os.environ.get(
    "BMKG_API_URL_AUTOGEMPA",
    "https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json"
)

POLLING_INTERVAL = int(
    os.environ.get("POLLING_INTERVAL", "30")
)


class ProducerAutogempaBMKG:
    """Producer untuk autogempa.json (1 event terbaru BMKG)."""

    def __init__(self):
        self.producer = None
        self.last_shakemap = None
        self.total_terkirim = 0
        self.koneksi_kafka()

    def koneksi_kafka(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=5,
                retry_backoff_ms=1000,
                acks="all",
                compression_type="gzip"
            )
            logger.info(f"Berhasil konek ke Kafka di {KAFKA_BOOTSTRAP_SERVERS}")
            self.kirim_log("PRODUCER_START", "Producer autogempa mulai berjalan", "INFO")

        except NoBrokersAvailable:
            logger.error(f"Tidak ada broker Kafka di {KAFKA_BOOTSTRAP_SERVERS}")
            raise
        except Exception as e:
            logger.error(f"Gagal konek ke Kafka: {e}")
            self.kirim_log("PRODUCER_ERROR", f"Koneksi Kafka gagal: {str(e)}", "ERROR")
            raise

    def kirim_log(self, log_type: str, pesan: str, level: str = "INFO"):
        if not self.producer:
            return

        log_event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "rosbd_producer_autogempa",
            "log_type": log_type,
            "level": level,
            "message": pesan
        }

        try:
            self.producer.send(KAFKA_TOPIC_LOGS, value=log_event)
            logger.debug(f"Log sent: {log_type} - {pesan}")
        except Exception as e:
            logger.warning(f"Gagal kirim log: {e}")

    def ambil_data_autogempa(self) -> Optional[Dict]:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            url = f"{BMKG_API_URL_AUTOGEMPA}?t={int(time.time() * 1000)}"
            response = requests.get(url, timeout=30, headers=headers)
            response.raise_for_status()
            data = response.json()

            gempa = data.get("Infogempa", {}).get("gempa", {})
            if gempa:
                return gempa

            logger.warning("autogempa.json: format tidak dikenal atau kosong")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Gagal ambil data autogempa: {e}")
            self.kirim_log("API_ERROR", f"Request autogempa gagal: {str(e)}", "ERROR")
            return None

        except json.JSONDecodeError as e:
            logger.error(f"JSON autogempa tidak valid: {e}")
            self.kirim_log("API_ERROR", f"Response JSON tidak valid: {str(e)}", "ERROR")
            return None

    def normalisasi_event(self, raw: Dict) -> Optional[Dict]:
        try:
            shakemap = raw.get("Shakemap", "")
            event_id = shakemap.replace(".mmi.jpg", "") if shakemap else "unknown"

            coords_raw = raw.get("Coordinates", "0,0")
            parts = coords_raw.split(",")
            latitude = float(parts[0].strip()) if len(parts) > 0 else 0.0
            longitude = float(parts[1].strip()) if len(parts) > 1 else 0.0

            magnitude = float(raw.get("Magnitude", 0))

            depth_str = raw.get("Kedalaman", "0 km")
            depth = int(float(depth_str.replace(" km", "").replace(" Km", "").strip()))

            tanggal = raw.get("Tanggal", "")
            jam = raw.get("Jam", "")
            datetime_wib = f"{tanggal} {jam}" if tanggal and jam else ""

            region = raw.get("Wilayah", "Tidak diketahui")

            normalized = {
                "event_id": event_id,
                "datetime": datetime_wib,
                "latitude": latitude,
                "longitude": longitude,
                "magnitude": round(magnitude, 1),
                "depth": depth,
                "region": region,
                "source": "BMKG_AUTOGEMPA",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            return normalized

        except Exception as e:
            logger.warning(f"Gagal normalisasi event autogempa: {e}")
            return None

    def kirim_event(self, event: Dict):
        event_id = event["event_id"]

        try:
            future = self.producer.send(
                KAFKA_TOPIC_EARTHQUAKE,
                key=event_id,
                value=event
            )

            record_metadata = future.get(timeout=10)

            logger.info(
                f"[TERKIRIM] Mag {event['magnitude']} | "
                f"{event['region']} | "
                f"Koordinat: {event['latitude']}, {event['longitude']} | "
                f"Depth: {event['depth']}km | "
                f"Waktu: {event['datetime']}"
            )

            self.total_terkirim += 1

        except KafkaError as e:
            logger.error(f"Gagal kirim event {event_id}: {e}")
            self.kirim_log("KAFKA_ERROR", f"Gagal kirim event {event_id}: {str(e)}", "ERROR")

    def polling_sekali(self):
        raw = self.ambil_data_autogempa()

        if raw is None:
            self.kirim_log("FETCH_FAILED", "Gagal ambil data autogempa", "WARNING")
            return

        if not raw:
            logger.info("autogempa: data kosong")
            return

        shakemap = raw.get("Shakemap", "")

        if shakemap == self.last_shakemap:
            logger.debug(f"autogempa: tidak ada event baru (shakemap: {shakemap})")
            return

        normalized = self.normalisasi_event(raw)
        if not normalized:
            return

        self.kirim_event(normalized)
        self.last_shakemap = shakemap

        logger.info(
            f"autogempa cycle: 1 dikirim, "
            f"total terkirim: {self.total_terkirim}"
        )
        self.kirim_log(
            "FETCH_SUCCESS",
            f"autogempa cycle: 1 dikirim, total: {self.total_terkirim}",
            "INFO"
        )

    def jalankan(self):
        logger.info(f"Producer Autogempa BMKG mulai. Polling setiap {POLLING_INTERVAL} detik")
        logger.info(f"Endpoint: {BMKG_API_URL_AUTOGEMPA}")

        try:
            while True:
                self.polling_sekali()
                time.sleep(POLLING_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Producer autogempa dihentikan oleh user")
            self.kirim_log("PRODUCER_STOP", "Producer autogempa dihentikan user", "INFO")

        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer ditutup")


if __name__ == "__main__":
    producer = ProducerAutogempaBMKG()
    producer.jalankan()
