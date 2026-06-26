"""
TBS_ROSBD - Producer Data Gempa InaTEWS (Live30 XML)
Flow: live30event.xml -> Kafka Producer
- Sumber: https://bmkg-content-inatews.storage.googleapis.com/live30event.xml
- Polling setiap POLLING_INTERVAL detik
- Deduplication berdasarkan BMKG eventID
- Semua magnitude, real-time (sumber BMKG website)
"""

import os
import sys
import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

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
logger = logging.getLogger("inatews_producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

KAFKA_TOPIC_EARTHQUAKE = os.environ.get(
    "KAFKA_TOPIC_EARTHQUAKE", "earthquake-events"
)

KAFKA_TOPIC_LOGS = os.environ.get(
    "KAFKA_TOPIC_LOGS", "system-logs"
)

BMKG_API_URL_LIVE30 = os.environ.get(
    "BMKG_API_URL_LIVE30",
    "https://bmkg-content-inatews.storage.googleapis.com/live30event.xml"
)

POLLING_INTERVAL = int(os.environ.get("POLLING_INTERVAL", "30"))


class ProducerInaTEWS:
    """Producer untuk live30event.xml (semua event, real-time)."""

    def __init__(self):
        self.producer = None
        self.event_sudah_dikirim = set()
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
            self.kirim_log("PRODUCER_START", "Producer live30 mulai berjalan", "INFO")

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
            "service": "rosbd_producer_live30",
            "log_type": log_type,
            "level": level,
            "message": pesan
        }

        try:
            self.producer.send(KAFKA_TOPIC_LOGS, value=log_event)
        except Exception:
            pass

    def ambil_data_live30(self) -> Optional[List[Dict]]:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            url = f"{BMKG_API_URL_LIVE30}?t={int(time.time() * 1000)}"
            response = requests.get(url, timeout=30, headers=headers)
            response.raise_for_status()

            root = ET.fromstring(response.content)
            events = []

            for gempa in root.findall("gempa"):
                event = {
                    "eventid": gempa.findtext("eventid", ""),
                    "waktu": gempa.findtext("waktu", ""),
                    "lintang": gempa.findtext("lintang", "0"),
                    "bujur": gempa.findtext("bujur", "0"),
                    "mag": gempa.findtext("mag", "0"),
                    "dalam": gempa.findtext("dalam", "0"),
                    "area": gempa.findtext("area", "Tidak diketahui"),
                }
                events.append(event)

            return events

        except ET.ParseError as e:
            logger.error(f"XML live30 tidak valid: {e}")
            self.kirim_log("API_ERROR", f"Parse XML gagal: {str(e)}", "ERROR")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Gagal ambil live30: {e}")
            self.kirim_log("API_ERROR", f"Request live30 gagal: {str(e)}", "ERROR")
            return None

    def normalisasi_event(self, raw: Dict) -> Optional[Dict]:
        try:
            event_id = raw.get("eventid", "unknown")

            lat = float(raw.get("lintang", 0))
            lon = float(raw.get("bujur", 0))
            mag = float(raw.get("mag", 0))
            depth = int(float(raw.get("dalam", 0)))

            waktu_str = raw.get("waktu", "")
            datetime_wib = ""
            if waktu_str:
                try:
                    waktu_clean = waktu_str.strip().replace("  ", " ")
                    dt_utc = datetime.strptime(waktu_clean, "%Y/%m/%d %H:%M:%S.%f")
                    dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
                    dt_wib = dt_utc.astimezone(ZoneInfo("Asia/Jakarta"))
                    datetime_wib = dt_wib.strftime("%Y-%m-%d %H:%M:%S WIB")
                except Exception:
                    datetime_wib = waktu_str

            region = raw.get("area", "Tidak diketahui")

            normalized = {
                "event_id": event_id,
                "datetime": datetime_wib,
                "latitude": lat,
                "longitude": lon,
                "magnitude": round(mag, 1),
                "depth": depth,
                "region": region,
                "source": "BMKG_LIVE30",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            return normalized

        except Exception as e:
            logger.warning(f"Gagal normalisasi event: {e}")
            return None

    def kirim_event(self, event: Dict):
        event_id = event["event_id"]

        try:
            future = self.producer.send(
                KAFKA_TOPIC_EARTHQUAKE,
                key=event_id,
                value=event
            )

            future.get(timeout=10)

            logger.info(
                f"[TERKIRIM] Mag {event['magnitude']} | "
                f"{event['region']} | "
                f"Koordinat: {event['latitude']}, {event['longitude']} | "
                f"Depth: {event['depth']}km | "
                f"Waktu: {event['datetime']}"
            )

            self.event_sudah_dikirim.add(event_id)
            self.total_terkirim += 1

            if len(self.event_sudah_dikirim) > 10000:
                self.event_sudah_dikirim = set(list(self.event_sudah_dikirim)[-5000:])

        except KafkaError as e:
            logger.error(f"Gagal kirim event {event_id}: {e}")
            self.kirim_log("KAFKA_ERROR", f"Gagal kirim event {event_id}: {str(e)}", "ERROR")

    def polling_sekali(self):
        raw_events = self.ambil_data_live30()

        if raw_events is None:
            self.kirim_log("FETCH_FAILED", "Gagal ambil live30", "WARNING")
            return

        if not raw_events:
            logger.info("live30: data kosong")
            return

        jumlah_baru = 0
        jumlah_dedup = 0

        for raw in raw_events:
            event_id = raw.get("eventid", "")

            if event_id in self.event_sudah_dikirim:
                jumlah_dedup += 1
                continue

            normalized = self.normalisasi_event(raw)
            if not normalized:
                jumlah_dedup += 1
                continue

            self.kirim_event(normalized)
            jumlah_baru += 1

        if jumlah_baru == 0:
            logger.debug(f"live30: tidak ada event baru ({jumlah_dedup} dedup)")
        else:
            logger.info(
                f"live30 cycle: {jumlah_baru} baru, {jumlah_dedup} dedup, "
                f"total unique: {len(self.event_sudah_dikirim)}"
            )
            self.kirim_log(
                "FETCH_SUCCESS",
                f"Live30: {jumlah_baru} baru, {jumlah_dedup} dedup, total: {len(self.event_sudah_dikirim)}",
                "INFO"
            )

    def jalankan(self):
        logger.info(f"Producer Live30 BMKG mulai. Polling setiap {POLLING_INTERVAL} detik")
        logger.info(f"Endpoint: {BMKG_API_URL_LIVE30}")

        try:
            while True:
                self.polling_sekali()
                time.sleep(POLLING_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Producer live30 dihentikan oleh user")
            self.kirim_log("PRODUCER_STOP", "Producer dihentikan user", "INFO")
            print(file=sys.stderr)

        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer ditutup")


if __name__ == "__main__":
    producer = ProducerInaTEWS()
    producer.jalankan()
