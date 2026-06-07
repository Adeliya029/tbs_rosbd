"""
TBS_ROSBD - Historical Web Scraper (FIX Version)
Scrapes BMKG historical earthquake data from berita_gempa.php
Saves raw data to MinIO.
"""

import os
import time
import json
import io
from datetime import datetime

import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from minio import Minio

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")
MINIO_SECURE = False

BUCKET_NAME = "historical-earthquake"

# BMKG Historical Scraping Config
BASE_URL = "http://202.90.198.42/gempa/berita_gempa.php"
START_PAGE = 1
END_PAGE = 10  # Untuk testing, ganti ke 238 untuk full

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/137.0 Safari/537.36"
    )
}

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
        print(f"✅ Bucket created: {BUCKET_NAME}")
    
    return client

def save_to_minio(client, data, filename, content_type="text/csv"):
    """Save data to MinIO."""
    if isinstance(data, pd.DataFrame):
        # Convert DataFrame to CSV
        csv_data = data.to_csv(index=False)
        data_bytes = csv_data.encode('utf-8')
    elif isinstance(data, dict):
        # Convert dict to JSON
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        data_bytes = json_data.encode('utf-8')
    else:
        # Assume it's already bytes
        data_bytes = data.encode('utf-8') if isinstance(data, str) else data
    
    data_buffer = io.BytesIO(data_bytes)
    data_buffer.seek(0)
    
    try:
        client.put_object(
            BUCKET_NAME,
            filename,
            data=data_buffer,
            length=len(data_bytes),
            content_type=content_type
        )
        print(f"  � Saved: {filename}")
        return True
    except Exception as e:
        print(f"  ❌ Failed to save {filename}: {e}")
        return False

def scrape_single_page(page: int):
    """Scrape a single page from BMKG."""
    url = f"{BASE_URL}?halaman={page}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Save raw HTML
        html_content = response.content
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.text, "lxml")
        
        # Find earthquake table
        target_table = None
        for table in soup.find_all("table"):
            headers_row = table.find("tr")
            if headers_row:
                text = headers_row.get_text(" ", strip=True)
                if "TANGGAL" in text.upper() and "WAKTU" in text.upper():
                    target_table = table
                    break
        
        if target_table is None:
            print(f"  ⚠️ Table not found on page {page}")
            return None, None
        
        # Extract table data
        records = []
        rows = target_table.find_all("tr")
        
        for row in rows[1:]:  # Skip header
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            
            if len(cols) >= 7:  # Minimum columns for earthquake data
                record = {
                    "page": page,
                    "no": cols[0] if len(cols) > 0 else "",
                    "tanggal": cols[1] if len(cols) > 1 else "",
                    "waktu": cols[2] if len(cols) > 2 else "",
                    "lintang": cols[3] if len(cols) > 3 else "",
                    "bujur": cols[4] if len(cols) > 4 else "",
                    "kedalaman_km": cols[5] if len(cols) > 5 else "",
                    "magnitude": cols[6] if len(cols) > 6 else "",
                    "scraped_at": datetime.now().isoformat()
                }
                
                if len(cols) > 7:
                    record["group_owner"] = cols[7]
                
                records.append(record)
        
        return html_content, records
        
    except Exception as e:
        print(f"  ❌ Error scraping page {page}: {e}")
        return None, None

def main():
    """Main scraping function."""
    print("=" * 60)
    print("TBS_ROSBD HISTORICAL WEB SCRAPER")
    print(f"Pages: {START_PAGE} to {END_PAGE}")
    print("=" * 60)
    
    # Initialize MinIO
    client = init_minio()
    
    all_records = []
    successful_pages = 0
    
    # Scrape pages
    for page in tqdm(range(START_PAGE, END_PAGE + 1), desc="Scraping"):
        html_content, records = scrape_single_page(page)
        
        if html_content and records:
            successful_pages += 1
            
            # Save raw HTML
            save_to_minio(
                client, 
                html_content, 
                f"raw_html/page_{page:03d}.html",
                "text/html"
            )
            
            # Save page data
            if records:
                df_page = pd.DataFrame(records)
                save_to_minio(
                    client,
                    df_page,
                    f"raw_data/page_{page:03d}.csv"
                )
                all_records.extend(records)
            
            print(f"  ✅ Page {page}: {len(records)} records")
        
        # Delay between requests
        time.sleep(0.5)
    
    # Save combined data
    if all_records:
        df_all = pd.DataFrame(all_records)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        save_to_minio(
            client,
            df_all,
            f"raw_data/historical_combined_{timestamp}.csv"
        )
        
        # Save summary
        summary = {
            "scraped_at": datetime.now().isoformat(),
            "total_pages": END_PAGE - START_PAGE + 1,
            "successful_pages": successful_pages,
            "total_records": len(df_all),
            "start_page": START_PAGE,
            "end_page": END_PAGE,
            "files": {
                "html_pages": [f"raw_html/page_{p:03d}.html" for p in range(START_PAGE, END_PAGE + 1)],
                "data_pages": [f"raw_data/page_{p:03d}.csv" for p in range(START_PAGE, END_PAGE + 1)],
                "combined": f"raw_data/historical_combined_{timestamp}.csv"
            }
        }
        
        save_to_minio(
            client,
            summary,
            "raw_data/scraping_summary.json",
            "application/json"
        )
        
        print("\n" + "=" * 60)
        print("✅ SCRAPING COMPLETE!")
        print("=" * 60)
        print(f"📊 Statistics:")
        print(f"  Total pages attempted: {END_PAGE - START_PAGE + 1}")
        print(f"  Successful pages: {successful_pages}")
        print(f"  Total records scraped: {len(df_all)}")
        print(f"  Saved to bucket: {BUCKET_NAME}")
        print("\n📁 Files created in MinIO:")
        print(f"  • HTML files: {successful_pages} files in raw_html/")
        print(f"  • CSV files: {successful_pages} files in raw_data/")
        print(f"  • Combined file: historical_combined_{timestamp}.csv")
        print(f"  • Summary: scraping_summary.json")
        print("=" * 60)
        
        # Show sample
        print("\n📋 Sample data (first 3 records):")
        sample_cols = ["no", "tanggal", "waktu", "lintang", "bujur", "magnitude", "kedalaman_km"]
        if not df_all.empty:
            print(df_all[sample_cols].head(3).to_string(index=False))
    
    else:
        print("\n❌ No data scraped. Check connection or website structure.")

if __name__ == "__main__":
    main()