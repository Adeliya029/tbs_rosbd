"""
TBS_ROSBD - Realtime Producer Data Gempa BMKG
Flow: BMKG Realtime API → Kafka Producer
- Polling setiap 3 detik (POLLING_INTERVAL=3)
- Publish ke dua topic: earthquake-events dan system-logs
- Deduplication berdasarkan konten (lat, lon, mag, depth, datetime)
"""

import os
import sys
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Setup logging
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
logger = logging.getLogger("rosbd_producer")

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

BMKG_API_URL = os.environ.get(
    "BMKG_API_URL", 
    "https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json"
)

# Polling interval 3 detik
POLLING_INTERVAL = int(
    os.environ.get("POLLING_INTERVAL", "3")
)


class ProducerGempaBMKG:
    """Producer untuk mengambil data gempa dari BMKG dan mengirim ke Kafka."""

    def __init__(self):
        self.producer = None
        self.event_sudah_dikirim = set()  # Deduplication set
        self.koneksi_kafka()

    def koneksi_kafka(self):
        """Membuat koneksi ke Kafka."""
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
            self.kirim_log("PRODUCER_START", "Producer mulai berjalan", "INFO")

        except NoBrokersAvailable:
            logger.error(f"Tidak ada broker Kafka di {KAFKA_BOOTSTRAP_SERVERS}")
            raise
        except Exception as e:
            logger.error(f"Gagal konek ke Kafka: {e}")
            self.kirim_log("PRODUCER_ERROR", f"Koneksi Kafka gagal: {str(e)}", "ERROR")
            raise

    def kirim_log(self, log_type: str, pesan: str, level: str = "INFO"):
        """Kirim log sistem ke topic system-logs di Kafka."""
        if not self.producer:
            return

        log_event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "rosbd_producer",
            "log_type": log_type,
            "level": level,
            "message": pesan
        }

        try:
            self.producer.send(KAFKA_TOPIC_LOGS, value=log_event)
            logger.debug(f"Log sent: {log_type} - {pesan}")
        except Exception as e:
            logger.warning(f"Gagal kirim log: {e}")

    def ambil_data_bmkg(self) -> Optional[List[Dict]]:
        """Ambil data gempa dari BMKG API."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(BMKG_API_URL, timeout=30, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Parse struktur JSON BMKG
            if "Infogempa" in data and "gempa" in data["Infogempa"]:
                events = data["Infogempa"]["gempa"]

                # autogempa return dict tunggal
                if isinstance(events, dict):
                    events = [events]

                return events

            return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Gagal ambil data dari BMKG: {e}")
            self.kirim_log("API_ERROR", f"Request BMKG gagal: {str(e)}", "ERROR")
            return None

        except json.JSONDecodeError as e:
            logger.error(f"JSON dari BMKG tidak valid: {e}")
            self.kirim_log("API_ERROR", f"Response JSON tidak valid: {str(e)}", "ERROR")
            return None

    def normalisasi_event(self, raw_event: Dict) -> Optional[Dict]:
        """Normalisasi data raw BMKG ke format standar."""
        try:
            # Parse datetime dari format BMKG
            datetime_str = raw_event.get(
                "DateTime", 
                raw_event.get("Tanggal", "") + " " + raw_event.get("Jam", "")
            )

            # Parse koordinat
            coordinates = raw_event.get("Coordinates", "0,0").split(",")
            latitude = float(coordinates[0].strip()) if len(coordinates) > 0 else 0.0
            longitude = float(coordinates[1].strip()) if len(coordinates) > 1 else 0.0

            # Parse magnitude dan kedalaman
            magnitude_str = raw_event.get("Magnitude", "0.0")
            magnitude = float(magnitude_str.replace("SR", "").strip()) if isinstance(magnitude_str, str) else float(magnitude_str)

            depth_str = raw_event.get("Kedalaman", "0")
            depth = int(depth_str.replace("km", "").strip()) if isinstance(depth_str, str) else int(depth_str)

            # FIXED: Buat ID event yang STABIL berdasarkan konten (bukan timestamp)
            content_string = f"{datetime_str}_{latitude}_{longitude}_{magnitude}_{depth}"
            event_id = hashlib.md5(content_string.encode()).hexdigest()

            normalized = {
                "event_id": event_id,
                "datetime": datetime_str,
                "latitude": latitude,
                "longitude": longitude,
                "magnitude": magnitude,
                "depth": depth,
                "region": raw_event.get("Wilayah", raw_event.get("Dirasakan", "Tidak diketahui")),
                "felt_intensity": raw_event.get("Dirasakan", ""),
                "shakemap": raw_event.get("Shakemap", ""),
                "potensi": raw_event.get("Potensi", ""),
                "source": "BMKG_REALTIME",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            return normalized

        except Exception as e:
            logger.warning(f"Gagal normalisasi event: {e}")
            return None

    def kirim_event(self, event: Dict):
        """Kirim event gempa ke topic Kafka."""
        event_id = event["event_id"]

        # FIXED: Deduplication - cek apakah event dengan ID sama sudah dikirim
        if event_id in self.event_sudah_dikirim:
            logger.info(f"Event {event_id[:8]}... sudah dikirim sebelumnya, dilewati (dedup)")
            return

        try:
            future = self.producer.send(
                KAFKA_TOPIC_EARTHQUAKE,
                key=event_id,
                value=event
            )

            record_metadata = future.get(timeout=10)

            logger.info(
                f"Kirim event {event_id[:8]}... ke "
                f"partition {record_metadata.partition}, "
                f"offset {record_metadata.offset} | "
                f"Mag {event['magnitude']} | "
                f"Lat {event['latitude']} Lon {event['longitude']} | "
                f"Depth {event['depth']}km"
            )

            self.event_sudah_dikirim.add(event_id)

            # Batasi ukuran set agar tidak terlalu besar
            if len(self.event_sudah_dikirim) > 10000:
                self.event_sudah_dikirim = set(list(self.event_sudah_dikirim)[-5000:])

        except KafkaError as e:
            logger.error(f"Gagal kirim event {event_id}: {e}")
            self.kirim_log("KAFKA_ERROR", f"Gagal kirim event {event_id}: {str(e)}", "ERROR")

    def polling_sekali(self):
        """Eksekusi satu siklus polling."""
        logger.info("Polling BMKG API...")

        raw_events = self.ambil_data_bmkg()

        if raw_events is None:
            self.kirim_log("FETCH_FAILED", "Gagal ambil data BMKG", "WARNING")
            return

        if not raw_events:
            logger.info("Tidak ada data gempa baru")
            self.kirim_log("NO_DATA", "API tidak return data gempa", "INFO")
            return

        jumlah_terkirim = 0
        jumlah_dedup = 0
        for raw_event in raw_events:
            normalized = self.normalisasi_event(raw_event)
            if normalized:
                event_id = normalized["event_id"]
                if event_id in self.event_sudah_dikirim:
                    jumlah_dedup += 1
                else:
                    self.kirim_event(normalized)
                    jumlah_terkirim += 1

        logger.info(
            f"Cycle summary: {jumlah_terkirim} dikirim, "
            f"{jumlah_dedup} dilewati (duplicate), "
            f"total unique events: {len(self.event_sudah_dikirim)}"
        )
        self.kirim_log(
            "FETCH_SUCCESS", 
            f"Cycle: {jumlah_terkirim} dikirim, {jumlah_dedup} dedup, total unique: {len(self.event_sudah_dikirim)}", 
            "INFO"
        )

    def jalankan(self):
        """Loop utama dengan simple polling."""
        logger.info(f"Producer TBS_ROSBD mulai. Polling setiap {POLLING_INTERVAL} detik")
        logger.info(f"Deduplication aktif: event dengan lat/lon/mag/depth/datetime sama akan dilewati")

        try:
            while True:
                self.polling_sekali()
                time.sleep(POLLING_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Producer dihentikan oleh user")
            self.kirim_log("PRODUCER_STOP", "Producer dihentikan user", "INFO")

        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer ditutup")


if __name__ == "__main__":
    producer = ProducerGempaBMKG()
    producer.jalankan()