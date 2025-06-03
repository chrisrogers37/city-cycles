import os
import re
import asyncio
from datetime import datetime
from urllib.parse import urljoin
import shutil
from data_ingestion.utils import upload_to_s3
from playwright.async_api import async_playwright

LONDON_BASE_URL = "https://cycling.data.tfl.gov.uk/"
USAGE_STATS_PATH = "usage-stats/"
LOCAL_TMP_DIR = "/tmp/london_bike/"
S3_PREFIX = "london_csv"

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

async def list_and_download_london_csv_files():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(LONDON_BASE_URL)
        # Click the usage-stats/ link
        await page.click('a[href="usage-stats/"]')
        await page.wait_for_load_state('networkidle')
        # Get all CSV links
        links = await page.query_selector_all('a')
        current_year = datetime.now().year
        pattern = re.compile(r"(Journey\s?Data\s?Extract.*\.csv)", re.IGNORECASE)
        files = []
        for link in links:
            href = await link.get_attribute('href')
            if href and pattern.search(href):
                year_match = re.search(r"(20\d{2})", href)
                if year_match and int(year_match.group(1)) >= 2019:
                    file_url = urljoin(LONDON_BASE_URL + USAGE_STATS_PATH, href)
                    files.append((file_url, href))
        print(f"Matched {len(files)} files for 2019 and later.")
        print(f"Sample files: {[f[0] for f in files[:5]]}")
        for file_url, filename in files:
            local_path = os.path.join(LOCAL_TMP_DIR, os.path.basename(filename))
            print(f"Downloading {file_url} to {local_path} ...")
            # Download file using Playwright's page API
            download = await page.wait_for_event('download', lambda: page.click(f'a[href="{filename}"]'))
            await download.save_as(local_path)
            s3_key = f"{S3_PREFIX}/{os.path.basename(filename)}"
            upload_to_s3(local_path, s3_key)
            os.remove(local_path)
            print(f"Uploaded and cleaned up: {local_path}")
            await asyncio.sleep(1)  # Respectful delay
        await browser.close()
    # Clean up temp dir
    try:
        shutil.rmtree(LOCAL_TMP_DIR)
    except Exception:
        pass

def process_and_upload_london_files():
    asyncio.run(list_and_download_london_csv_files())

if __name__ == "__main__":
    process_and_upload_london_files()