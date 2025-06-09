from dotenv import load_dotenv
load_dotenv()
import os
import re
import asyncio
from datetime import datetime
from urllib.parse import urljoin
import shutil
from data_ingestion.utils import upload_to_s3, file_exists_in_s3
from playwright.async_api import async_playwright
import time
import requests

LONDON_BASE_URL = "https://cycling.data.tfl.gov.uk/"
LOCAL_TMP_DIR = "/tmp/extracted_bike_ride_csvs/london/"
RAW_CSV_PREFIX = "extracted_bike_ride_csvs/london"  # Store raw CSVs here

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

async def list_london_csv_files():
    """
    List all available CSV files from the TfL website.
    Returns a list of tuples (file_url, filename)
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        await page.goto(LONDON_BASE_URL)
        
        # Scroll the #full-width-content container for up to 30 seconds
        start_time = time.time()
        for _ in range(30):
            await page.evaluate("""
                var el = document.querySelector('#full-width-content');
                if (el) el.scrollTop = el.scrollHeight;
            """)
            await asyncio.sleep(1)
            
        # Now collect all links
        await page.wait_for_selector('tbody#tbody-content a', timeout=20000)
        links = await page.query_selector_all('tbody#tbody-content a')
        
        pattern = re.compile(r"(Journey\s?Data\s?Extract.*\.csv)", re.IGNORECASE)
        files = []
        
        for link in links:
            href = await link.get_attribute('href')
            if href and pattern.search(href):
                year_match = re.search(r"(20\d{2})", href)
                if year_match and int(year_match.group(1)) >= 2019:
                    file_url = urljoin(LONDON_BASE_URL, href)
                    files.append((file_url, href))
                    
        await browser.close()
        return files

def download_and_store_csv(file_url: str, filename: str) -> bool:
    """
    Download a CSV file and store it in the raw CSV folder in S3.
    Returns True if the file was downloaded and stored, False if it already exists.
    """
    # Handle XLS files that are actually CSVs (apparent bug in TfL website)
    if filename.lower().endswith('.xls'):
        s3_filename = os.path.basename(filename)[:-4] + '.csv'
    else:
        s3_filename = os.path.basename(filename)
    
    s3_key = f"{RAW_CSV_PREFIX}/{s3_filename}"
    
    # Check if we already have this CSV file
    if file_exists_in_s3(s3_key):
        print(f"CSV file already exists in S3: {s3_key}")
        return False
        
    try:
        local_path = os.path.join(LOCAL_TMP_DIR, os.path.basename(filename))
        print(f"Downloading {file_url} to {local_path} ...")
        
        # Download the file
        response = requests.get(file_url)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(response.content)
            
        # If the file ends with .xls but is actually a CSV, rename it
        if local_path.lower().endswith('.xls'):
            new_local_path = local_path[:-4] + '.csv'
            os.rename(local_path, new_local_path)
            local_path = new_local_path
            
        # Upload to S3
        upload_to_s3(local_path, s3_key)
        print(f"Stored CSV file in S3: {s3_key}")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to process {file_url}: {e}")
        return False
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)

async def download_all_csvs():
    """
    Download all CSV files from TfL and store them in S3.
    """
    print(f"Using S3 bucket: {os.environ.get('S3_BUCKET')}")
    print("Attempting to download all CSV files from TfL...")
    files = await list_london_csv_files()
    print(f"Found {len(files)} files to process.")
    
    downloaded_count = 0
    skipped_count = 0
    
    for file_url, filename in files:
        if download_and_store_csv(file_url, filename):
            downloaded_count += 1
        else:
            skipped_count += 1
        await asyncio.sleep(1)  # Respectful delay
        
    print(f"\nDownload Summary:")
    print(f"Total files found: {len(files)}")
    print(f"New files downloaded: {downloaded_count}")
    print(f"Files already in S3: {skipped_count}")

def process_and_upload_london_files():
    asyncio.run(download_all_csvs())

if __name__ == "__main__":
    process_and_upload_london_files()