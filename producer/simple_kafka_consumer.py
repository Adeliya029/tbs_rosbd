"""
Simple Kafka Consumer untuk testing.
Mendengarkan topic earthquake-events dan system-logs.
"""

import json
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("kafka_consumer")

def main():
    """Main consumer function."""
    
    print("=" * 50)
    print("Kafka Consumer - Testing")
    print("=" * 50)
    
    # Consumer untuk earthquake-events
    consumer_events = KafkaConsumer(
        'earthquake-events',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Consumer untuk system-logs  
    consumer_logs = KafkaConsumer(
        'system-logs',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Consumer siap. Menunggu data...")
    print("Tekan Ctrl+C untuk berhenti.")
    print("-" * 50)
    
    try:
        # Poll kedua consumer
        for message in consumer_events:
            print("\n[EARTHQUAKE EVENT]")
            print(f"Topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Key: {message.key}")
            print("Data:")
            data = message.value
            print(f"  Event ID: {data.get('event_id')}")
            print(f"  Waktu: {data.get('datetime')}")
            print(f"  Magnitude: {data.get('magnitude')}")
            print(f"  Lokasi: {data.get('latitude')}, {data.get('longitude')}")
            print(f"  Wilayah: {data.get('region')}")
            
        for message in consumer_logs:
            print("\n[SYSTEM LOG]")
            print(f"Topic: {message.topic}")
            print(f"Level: {message.value.get('level')}")
            print(f"Service: {message.value.get('service')}")
            print(f"Message: {message.value.get('message')}")
            
    except KeyboardInterrupt:
        print("\nConsumer dihentikan.")
    finally:
        consumer_events.close()
        consumer_logs.close()
        print("Consumer ditutup.")

if __name__ == "__main__":
    main()