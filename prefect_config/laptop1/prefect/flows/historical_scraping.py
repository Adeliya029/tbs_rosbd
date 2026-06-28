import os
import io
import re
import time
from datetime import datetime

import requests
import pandas as pd
from minio import Minio
from prefect import flow, task

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "100.122.2.11:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "False").lower() == "true"
BUCKET_NAME = "historical-earthquake"
BASE_URL = os.environ.get("BMKG_SCRAPE_URL", "http://202.90.198.42/gempa/berita_gempa.php")
START_PAGE = 1
END_PAGE = 238
BASE_DELAY = float(os.environ.get("SCRAPE_DELAY", "0.5"))
SCRAPE_TIMEOUT = int(os.environ.get("SCRAPE_TIMEOUT", "30"))
MAX_RETRIES = 3
RETRY_DELAY = 5


@task(retries=2, retry_delay_seconds=10)
def init_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket created: {BUCKET_NAME}")
    return client


@task
def save_to_minio(client, data_bytes, filename, content_type):
    if not data_bytes:
        return
    buffer = io.BytesIO(data_bytes)
    buffer.seek(0)
    client.put_object(
        BUCKET_NAME, filename,
        data=buffer, length=len(data_bytes),
        content_type=content_type
    )
    print(f"  Saved: {filename} ({len(data_bytes)} bytes)")


@task(retries=MAX_RETRIES - 1, retry_delay_seconds=RETRY_DELAY)
def scrape_page(page_num):
    url = f"{BASE_URL}?page={page_num}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    response = requests.get(url, headers=headers, timeout=SCRAPE_TIMEOUT)
    response.raise_for_status()

    records = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', response.text, re.DOTALL)

    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 7:
            continue

        clean = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
        if not clean[0] or clean[0] in ['NO', '']:
            continue

        try:
            lat_match = re.match(r'([\d.]+)\s+(LU|LS)', clean[3])
            latitude = None
            if lat_match:
                latitude = float(lat_match.group(1))
                if lat_match.group(2) == 'LS':
                    latitude = -latitude

            lon_match = re.match(r'([\d.]+)\s+(BT|BB)', clean[4])
            longitude = None
            if lon_match:
                longitude = float(lon_match.group(1))
                if lon_match.group(2) == 'BB':
                    longitude = -longitude

            depth_match = re.search(r'([\d.]+)', clean[5])
            depth = float(depth_match.group(1)) if depth_match else None

            mag_match = re.search(r'([\d.]+)', clean[6])
            magnitude = float(mag_match.group(1)) if mag_match else None

            records.append({
                "no": clean[0],
                "tanggal": clean[1],
                "waktu": clean[2],
                "latitude": latitude,
                "longitude": longitude,
                "kedalaman_km": depth,
                "magnitudo": magnitude,
                "wilayah": clean[7] if len(clean) > 7 else "",
                "source": "berita_gempa",
                "scraped_at": datetime.now().isoformat()
            })
        except (ValueError, IndexError):
            continue

    return records


@flow(name="historical-scraping")
def historical_scraping_flow():
    print("=" * 60)
    print("TBS_ROSBD - Historical Data Scraper (Prefect Flow)")
    print(f"Source: {BASE_URL}")
    print(f"Pages: {START_PAGE} to {END_PAGE}")
    print(f"MinIO: {MINIO_ENDPOINT} / {BUCKET_NAME}")
    print("=" * 60)

    client = init_minio()
    all_records = []

    for page_num in range(START_PAGE, END_PAGE + 1):
        print(f"\nScraping page {page_num}/{END_PAGE}...")
        records = scrape_page(page_num)
        if records:
            all_records.extend(records)
            print(f"  {len(records)} records from page {page_num}")
        time.sleep(BASE_DELAY)

    if all_records:
        df = pd.DataFrame(all_records)

        csv_bytes = df.to_csv(index=False).encode('utf-8')
        save_to_minio(client, csv_bytes, "data_historical_raw.csv", "text/csv")

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="fastparquet")
        save_to_minio(client, parquet_buffer.getvalue(), "data_historical_raw.parquet", "application/parquet")

        print(f"\nScraping complete! {len(all_records)} records saved.")

    return len(all_records)
