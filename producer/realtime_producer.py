"""
TBS_ROSBD - Realtime Producer Data Gempa BMKG (InaTEWS)
Flow: BMKG InaTEWS CAP Alert + GeoJSON → Kafka Producer
- Polling realtime (datagempa.json) setiap POLLING_INTERVAL detik
- Polling batch (gempaQL.json) setiap BATCH_POLL_INTERVAL detik
- Publish ke dua topic: earthquake-events dan system-logs
- Deduplication berdasarkan BMKG event ID
- Sumber: https://inatews.bmkg.go.id (Google Cloud Storage)
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
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

# Real-time alert endpoint (CAP format) - update <1 menit, poll tiap 30 detik
BMKG_API_URL_REALTIME = os.environ.get(
    "BMKG_API_URL_REALTIME", 
    "https://bmkg-content-inatews.storage.googleapis.com/datagempa.json"
)

# Batch endpoint (GeoJSON) - update tiap ~10 menit, poll tiap 5 menit
BMKG_API_URL_BATCH = os.environ.get(
    "BMKG_API_URL_BATCH", 
    "https://bmkg-content-inatews.storage.googleapis.com/gempaQL.json"
)

POLLING_INTERVAL = int(
    os.environ.get("POLLING_INTERVAL", "30")
)


class ProducerGempaBMKG:
    """Producer untuk mengambil data gempa dari BMKG dan mengirim ke Kafka."""

    def __init__(self):
        self.producer = None
        self.event_sudah_dikirim = set()  # Deduplication set
        self.last_identifier = None  # Untuk tracking gempa baru dari datagempa
        self.last_batch_poll = 0  # Timestamp terakhir poll gempaQL
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
        """Ambil data gempa dari BMKG InaTEWS GeoJSON API."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(BMKG_API_URL, timeout=30, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Parse GeoJSON FeatureCollection dari InaTEWS
            if "features" in data:
                return data["features"]

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
        """Normalisasi data GeoJSON InaTEWS ke format standar."""
        try:
            props = raw_event.get("properties", {})
            geom = raw_event.get("geometry", {})
            coords = geom.get("coordinates", [0, 0, 0])

            event_id = props.get("id", "unknown")
            latitude = float(coords[1]) if len(coords) > 1 else 0.0
            longitude = float(coords[0]) if len(coords) > 0 else 0.0
            magnitude = float(props.get("mag", 0))
            depth = float(props.get("depth", 0))
            datetime_str = props.get("time", "")

            # Konversi UTC → WIB
            try:
                dt_utc = datetime.fromisoformat(datetime_str.replace(" ", "T"))
                dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
                dt_wib = dt_utc.astimezone(ZoneInfo("Asia/Jakarta"))
                datetime_wib = dt_wib.strftime("%Y-%m-%d %H:%M:%S WIB")
            except Exception:
                datetime_wib = datetime_str

            region = props.get("place", "Tidak diketahui")

            normalized = {
                "event_id": event_id,
                "datetime": datetime_wib,
                "latitude": latitude,
                "longitude": longitude,
                "magnitude": round(magnitude, 1),
                "depth": int(round(depth)),
                "region": region,
                "source": "BMKG_INATEWS",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            return normalized

        except Exception as e:
            logger.warning(f"Gagal normalisasi event: {e}")
            return None

    def kirim_event(self, event: Dict):
        """Kirim event gempa ke topic Kafka."""
        event_id = event["event_id"]

        if event_id in self.event_sudah_dikirim:
            logger.debug(f"DEDUP: {event_id[:8]} | {event['region']} | Mag {event['magnitude']}")
            return

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
        logger.info(f"Deduplication aktif: event dengan BMKG ID sama akan dilewati")

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