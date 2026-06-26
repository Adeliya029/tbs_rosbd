"""
TBS_ROSBD - Historical Scraper → MinIO (Laptop 2)
Flow: BMKG berita_gempa.php → MinIO (100.122.2.11:9000)
- Berdiri sendiri, terpisah dari realtime producer
- Hasil scraping langsung dikirim ke MinIO laptop 2 via Tailscale
- Output: data_historical_raw.csv + data_historical_raw.parquet
"""

import os
import time
import json
import re
from datetime import datetime

import requests
import pandas as pd
from minio import Minio
import io

# Configuration - MinIO Laptop 2 via Tailscale
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "100.122.2.11:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "False").lower() == "true"

BUCKET_NAME = "historical-earthquake"

# BMKG Historical Scraping Config
BASE_URL = "http://202.90.198.42/gempa/berita_gempa.php"
START_PAGE = 1
END_PAGE = 238

# Retry config
MAX_RETRIES = 3
BASE_DELAY = 3
RETRY_DELAY = 5


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


def save_bytes_to_minio(client, data_bytes, filename, content_type):
    if not data_bytes:
        return
    buffer = io.BytesIO(data_bytes)
    buffer.seek(0)
    client.put_object(
        BUCKET_NAME,
        filename,
        data=buffer,
        length=len(data_bytes),
        content_type=content_type
    )
    print(f"  Saved: {filename} ({len(data_bytes)} bytes)")


def parse_html_table(html_content):
    records = []

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_content, re.DOTALL)

    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        if len(cells) >= 7:
            clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

            if not clean_cells[0] or clean_cells[0] in ['NO', '']:
                continue

            try:
                date_str = clean_cells[1] if len(clean_cells) > 1 else ''
                time_str = clean_cells[2] if len(clean_cells) > 2 else ''

                lat_str = clean_cells[3] if len(clean_cells) > 3 else ''
                lat_match = re.match(r'([\d.]+)\s+(LU|LS)', lat_str)
                latitude = None
                if lat_match:
                    latitude = float(lat_match.group(1))
                    if lat_match.group(2) == 'LS':
                        latitude = -latitude

                lon_str = clean_cells[4] if len(clean_cells) > 4 else ''
                lon_match = re.match(r'([\d.]+)\s+(BT|BB)', lon_str)
                longitude = None
                if lon_match:
                    longitude = float(lon_match.group(1))
                    if lon_match.group(2) == 'BB':
                        longitude = -longitude

                depth_str = clean_cells[5] if len(clean_cells) > 5 else ''
                depth = None
                depth_match = re.search(r'([\d.]+)', depth_str)
                if depth_match:
                    depth = float(depth_match.group(1))

                mag_str = clean_cells[6] if len(clean_cells) > 6 else ''
                magnitude = None
                mag_match = re.search(r'([\d.]+)', mag_str)
                if mag_match:
                    magnitude = float(mag_match.group(1))

                region = clean_cells[7] if len(clean_cells) > 7 else 'pusat'

                record = {
                    "no": clean_cells[0],
                    "tanggal": date_str,
                    "waktu": time_str,
                    "latitude": latitude,
                    "longitude": longitude,
                    "kedalaman_km": depth,
                    "magnitudo": magnitude,
                    "wilayah": region,
                    "source": "berita_gempa",
                    "scraped_at": datetime.now().isoformat()
                }

                records.append(record)

            except (ValueError, IndexError):
                continue

    return records


def scrape_page(page_num):
    """Scrape a single page, with retry logic. Returns list of records."""
    url = f"{BASE_URL}?page={page_num}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            records = parse_html_table(response.text)
            return records

        except Exception as e:
            print(f"  Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_DELAY * attempt
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Giving up on page {page_num}")
                return []


def scrape_all_pages():
    print("=" * 60)
    print("TBS_ROSBD - Historical Data Scraper")
    print("Source: BMKG berita_gempa.php")
    print(f"Pages: {START_PAGE} to {END_PAGE}")
    print(f"MinIO : {MINIO_ENDPOINT} / {BUCKET_NAME}")
    print(f"Output: data_historical_raw.csv + data_historical_raw.parquet")
    print("=" * 60)

    client = init_minio()
    all_records = []
    total_pages_scraped = 0
    total_records = 0

    for page_num in range(START_PAGE, END_PAGE + 1):
        print(f"\nScraping page {page_num}/{END_PAGE}...")

        records = scrape_page(page_num)

        if records:
            all_records.extend(records)
            total_pages_scraped += 1
            total_records += len(records)
            print(f"  {len(records)} records from page {page_num}")
        else:
            print(f"  No records found on page {page_num}")

        time.sleep(BASE_DELAY)

    if all_records:
        df_all = pd.DataFrame(all_records)

        # CSV
        csv_bytes = df_all.to_csv(index=False).encode('utf-8')
        save_bytes_to_minio(client, csv_bytes, "data_historical_raw.csv", "text/csv")

        # Parquet
        parquet_buffer = io.BytesIO()
        df_all.to_parquet(parquet_buffer, index=False, engine="fastparquet")
        parquet_bytes = parquet_buffer.getvalue()
        save_bytes_to_minio(client, parquet_bytes, "data_historical_raw.parquet", "application/parquet")

        print("\n" + "=" * 60)
        print(f"Scraping complete!")
        print(f"Pages scraped: {total_pages_scraped}/{END_PAGE}")
        print(f"Total records: {total_records}")
        print(f"Saved to MinIO bucket: {BUCKET_NAME} @ {MINIO_ENDPOINT}")
        print("  - data_historical_raw.csv")
        print("  - data_historical_raw.parquet")
        print("=" * 60)

        print("\nSample data (first 3 records):")
        display_cols = ["no", "tanggal", "waktu", "latitude", "longitude", "magnitudo", "kedalaman_km", "wilayah"]
        available_cols = [c for c in display_cols if c in df_all.columns]
        print(df_all[available_cols].head(3).to_string())

    else:
        print("\nNo data scraped")


if __name__ == "__main__":
    scrape_all_pages()
