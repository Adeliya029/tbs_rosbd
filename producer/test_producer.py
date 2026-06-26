"""
Test Producer untuk BMKG InaTEWS GeoJSON API.
Mengambil data sekali dan kirim ke Kafka.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("test_producer")

# Konfigurasi
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_EARTHQUAKE = "earthquake-events"
BMKG_API_URL = "https://bmkg-content-inatews.storage.googleapis.com/gempaQL.json"

def koneksi_kafka():
    """Membuat koneksi ke Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
            acks="all"
        )
        logger.info(f"Berhasil konek ke Kafka di {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
        
    except Exception as e:
        logger.error(f"Gagal konek ke Kafka: {e}")
        raise

def ambil_data_bmkg() -> Optional[List[Dict]]:
    """Ambil data gempa dari BMKG API."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(BMKG_API_URL, timeout=30, headers=headers)
        response.raise_for_status()
        data = response.json()

        if "features" in data:
            return data["features"]

        return []

    except Exception as e:
        logger.error(f"Gagal ambil data dari BMKG: {e}")
        return None

def normalisasi_event(raw_event: Dict) -> Optional[Dict]:
    """Normalisasi data raw BMKG ke format standar."""
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
        region = props.get("place", "Tidak diketahui")

        normalized = {
            "event_id": event_id,
            "datetime": datetime_str,
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

def kirim_event(producer, event: Dict):
    """Kirim event gempa ke Kafka topic."""
    event_id = event["event_id"]
    
    try:
        future = producer.send(
            KAFKA_TOPIC_EARTHQUAKE,
            key=event_id,
            value=event
        )

        record_metadata = future.get(timeout=10)

        logger.info(
            f"Kirim event {event_id} ke "
            f"partition {record_metadata.partition}, "
            f"offset {record_metadata.offset}"
        )
        return True

    except KafkaError as e:
        logger.error(f"Gagal kirim event {event_id}: {e}")
        return False

def main():
    """Fungsi utama untuk testing."""
    print("=" * 50)
    print("Test Producer BMKG → Kafka")
    print("=" * 50)
    
    # Konek ke Kafka
    producer = koneksi_kafka()
    
    # Ambil data dari BMKG
    logger.info("Ambil data dari BMKG...")
    raw_events = ambil_data_bmkg()
    
    if raw_events is None:
        logger.error("Gagal ambil data BMKG")
        return
    
    if not raw_events:
        logger.info("Tidak ada data gempa")
        return
    
    logger.info(f"Ada {len(raw_events)} event dari BMKG")
    
    # Normalisasi dan kirim
    jumlah_berhasil = 0
    for raw_event in raw_events:
        normalized = normalisasi_event(raw_event)
        if normalized:
            print(f"\nEvent yang akan dikirim:")
            print(f"  Waktu: {normalized['datetime']}")
            print(f"  Magnitude: {normalized['magnitude']}")
            print(f"  Lokasi: {normalized['latitude']}, {normalized['longitude']}")
            print(f"  Wilayah: {normalized['region']}")
            
            if kirim_event(producer, normalized):
                jumlah_berhasil += 1
    
    producer.flush()
    producer.close()
    
    print("\n" + "=" * 50)
    print(f"Test selesai! {jumlah_berhasil} dari {len(raw_events)} event berhasil dikirim.")
    print("=" * 50)
    
    if jumlah_berhasil > 0:
        print("\nLangkah selanjutnya:")
        print("1. Buka terminal baru")
        print("2. Jalankan: python producer/simple_kafka_consumer.py")
        print("3. Harusnya muncul data yang baru dikirim")

if __name__ == "__main__":
    main()