from dotenv import load_dotenv
load_dotenv()
import os
import re
import asyncio
from datetime import datetime
from urllib.parse import urljoin
import shutil
from data_ingestion.utils import upload_to_s3
from playwright.async_api import async_playwright
import time
import requests

LONDON_BASE_URL = "https://cycling.data.tfl.gov.uk/"
LOCAL_TMP_DIR = "/tmp/london_bike/"
S3_PREFIX = "london_csv"

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

async def list_and_download_london_csv_files():
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
        # Print the outer HTML of the container for debugging
        container = await page.query_selector('#full-width-content')
        html = await page.evaluate("(el) => el ? el.outerHTML : 'NOT FOUND'", container)
        print("\n--- #full-width-content outerHTML (truncated) ---\n", html[:2000], "\n--- END ---\n")
        # Now collect all links
        await page.wait_for_selector('tbody#tbody-content a', timeout=20000)
        links = await page.query_selector_all('tbody#tbody-content a')
        print("All links found on page:")
        for link in links:
            href = await link.get_attribute('href')
            print(href)
        current_year = datetime.now().year
        pattern = re.compile(r"(Journey\s?Data\s?Extract.*\.csv)", re.IGNORECASE)
        files = []
        for link in links:
            href = await link.get_attribute('href')
            if href and pattern.search(href):
                year_match = re.search(r"(20\d{2})", href)
                if year_match and int(year_match.group(1)) >= 2019:
                    file_url = urljoin(LONDON_BASE_URL, href)
                    files.append((file_url, href))
        print(f"Matched {len(files)} files for 2019 and later.")
        print(f"Sample files: {[f[0] for f in files[:5]]}")
        # Use requests to download the files
        for file_url, filename in files:
            local_path = os.path.join(LOCAL_TMP_DIR, os.path.basename(filename))
            print(f"Downloading {file_url} to {local_path} ...")
            response = requests.get(file_url)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(response.content)
            s3_key = f"{S3_PREFIX}/{os.path.basename(filename)}"
            upload_to_s3(local_path, s3_key)
            os.remove(local_path)
            print(f"Uploaded and cleaned up: {local_path}")
            await asyncio.sleep(1)  # Respectful delay
        await browser.close()
    try:
        shutil.rmtree(LOCAL_TMP_DIR)
    except Exception:
        pass

def process_and_upload_london_files():
    asyncio.run(list_and_download_london_csv_files())

if __name__ == "__main__":
    process_and_upload_london_files()