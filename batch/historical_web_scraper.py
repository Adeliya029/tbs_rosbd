"""
TBS_ROSBD - Historical Web Scraper (Batch Job)
Flow: BMKG berita_gempa.php → MinIO (historical-earthquake/)
- Scraping halaman 1-238 dari berita_gempa.php
- Simpan per page: raw HTML + parsed CSV
- Tidak lewat Kafka (batch job terpisah dari realtime)
- Nantinya akan di-wrap oleh Prefect flow
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

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "False").lower() == "true"

BUCKET_NAME = "historical-earthquake"

# BMKG Historical Scraping Config
BASE_URL = "http://202.90.198.42/gempa/berita_gempa.php"
START_PAGE = 1
END_PAGE = 238


def init_minio():
    """Initialize MinIO client."""
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


def save_to_minio(client, data_bytes, filename, content_type="text/csv"):
    """Save bytes to MinIO."""
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
    """Parse earthquake data from HTML table in berita_gempa.php."""
    records = []

    # Extract all table rows
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_content, re.DOTALL)

    for row in rows:
        # Extract all table cells
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        if len(cells) >= 7:
            # Clean HTML tags from cell content
            clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

            # Skip header rows or empty rows
            if not clean_cells[0] or clean_cells[0] in ['NO', '']:
                continue

            try:
                # Parse date (format: DD/MM/YYYY)
                date_str = clean_cells[1] if len(clean_cells) > 1 else ''
                time_str = clean_cells[2] if len(clean_cells) > 2 else ''

                # Parse latitude (e.g., "2.5 LU" or "8.12 LS")
                lat_str = clean_cells[3] if len(clean_cells) > 3 else ''
                lat_match = re.match(r'([\d.]+)\s+(LU|LS)', lat_str)
                latitude = None
                if lat_match:
                    latitude = float(lat_match.group(1))
                    if lat_match.group(2) == 'LS':
                        latitude = -latitude

                # Parse longitude (e.g., "96.13 BT" or "107.89 BB")
                lon_str = clean_cells[4] if len(clean_cells) > 4 else ''
                lon_match = re.match(r'([\d.]+)\s+(BT|BB)', lon_str)
                longitude = None
                if lon_match:
                    longitude = float(lon_match.group(1))
                    if lon_match.group(2) == 'BB':
                        longitude = -longitude

                # Parse depth (remove "km" if present)
                depth_str = clean_cells[5] if len(clean_cells) > 5 else ''
                depth = None
                depth_match = re.search(r'([\d.]+)', depth_str)
                if depth_match:
                    depth = float(depth_match.group(1))

                # Parse magnitude
                mag_str = clean_cells[6] if len(clean_cells) > 6 else ''
                magnitude = None
                mag_match = re.search(r'([\d.]+)', mag_str)
                if mag_match:
                    magnitude = float(mag_match.group(1))

                # Region/location (if available)
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

            except (ValueError, IndexError) as e:
                # Skip malformed rows
                continue

    return records


def scrape_page(client, page_num):
    """Scrape a single page from berita_gempa.php."""
    url = f"{BASE_URL}?page={page_num}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Save raw HTML to MinIO: historical-earthquake/raw_html/page_0001.html
        html_bytes = response.text.encode('utf-8')
        save_to_minio(
            client, 
            html_bytes, 
            f"raw_html/page_{page_num:04d}.html",
            content_type="text/html"
        )

        # Parse table data
        records = parse_html_table(response.text)

        if records:
            df = pd.DataFrame(records)
            csv_bytes = df.to_csv(index=False).encode('utf-8')

            # Save parsed CSV to MinIO: historical-earthquake/raw_csv/page_0001.csv
            save_to_minio(
                client, 
                csv_bytes, 
                f"raw_csv/page_{page_num:04d}.csv",
                content_type="text/csv"
            )
            return records

        return []

    except Exception as e:
        print(f"  Error scraping page {page_num}: {e}")
        return []


def scrape_all_pages():
    """Scrape all pages from BMKG historical data."""
    print("=" * 60)
    print("TBS_ROSBD - Historical Data Scraper")
    print("Source: BMKG berita_gempa.php")
    print(f"Pages: {START_PAGE} to {END_PAGE}")
    print("Flow: BMKG → MinIO (historical-earthquake/)")
    print("Note: Batch job, tidak lewat Kafka")
    print("=" * 60)

    client = init_minio()
    all_records = []
    total_pages_scraped = 0
    total_records = 0

    for page_num in range(START_PAGE, END_PAGE + 1):
        print(f"\nScraping page {page_num}/{END_PAGE}...")

        records = scrape_page(client, page_num)

        if records:
            all_records.extend(records)
            total_pages_scraped += 1
            total_records += len(records)
            print(f"  {len(records)} records from page {page_num}")
        else:
            print(f"  No records found on page {page_num}")

        # Delay between requests to be respectful
        time.sleep(1)

    # Save combined data to MinIO: historical-earthquake/combined/
    if all_records:
        df_all = pd.DataFrame(all_records)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_bytes = df_all.to_csv(index=False).encode('utf-8')

        save_to_minio(
            client, 
            csv_bytes, 
            f"combined/all_pages_combined_{timestamp}.csv",
            content_type="text/csv"
        )

        print("\n" + "=" * 60)
        print(f"Scraping complete!")
        print(f"Pages scraped: {total_pages_scraped}/{END_PAGE}")
        print(f"Total records: {total_records}")
        print(f"Saved to MinIO bucket: {BUCKET_NAME}")
        print("=" * 60)

        # Show sample
        print("\nSample data (first 3 records):")
        display_cols = ["no", "tanggal", "waktu", "latitude", "longitude", "magnitudo", "kedalaman_km", "wilayah"]
        available_cols = [c for c in display_cols if c in df_all.columns]
        print(df_all[available_cols].head(3).to_string())

    else:
        print("\nNo data scraped")


if __name__ == "__main__":
    scrape_all_pages()