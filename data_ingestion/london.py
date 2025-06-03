import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from time import sleep
import shutil
from data_ingestion.utils import upload_to_s3

LONDON_BASE_URL = "https://cycling.data.tfl.gov.uk/"
LOCAL_TMP_DIR = "/tmp/london_bike/"
S3_PREFIX = "london_csv"

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

def list_london_csv_files():
    print(f"Fetching file list from {LONDON_BASE_URL} ...")
    response = requests.get(LONDON_BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    pattern = re.compile(r"(Journey\s?Data\s?Extract.*\.csv)", re.IGNORECASE)
    files = []
    for link in soup.find_all("a", href=True):
        filename = link['href']
        print("Found link:", filename)  # Debug print
        if pattern.search(filename):
            print("Matched pattern:", filename)  # Debug print
            year_match = re.search(r"(20\d{2})", filename)
            if year_match:
                print("Year found:", year_match.group(1))  # Debug print
            if year_match and int(year_match.group(1)) >= 2019:
                files.append(filename)
    print(f"Matched {len(files)} files for 2019 and later.")
    print(f"Sample files: {files[:5]}")
    return files

def download_file(url, dest_path):
    print(f"Downloading {url} to {dest_path} ...")
    with requests.get(url, stream=True) as r:
        with open(dest_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def process_and_upload_london_files():
    files = list_london_csv_files()
    for filename in files:
        file_url = urljoin(LONDON_BASE_URL, filename)
        local_path = os.path.join(LOCAL_TMP_DIR, filename)
        try:
            download_file(file_url, local_path)
            s3_key = f"{S3_PREFIX}/{filename}"
            upload_to_s3(local_path, s3_key)
            os.remove(local_path)
            print(f"Uploaded and cleaned up: {local_path}")
        except Exception as e:
            print(f"ERROR: Failed to process {filename}: {e}")
        sleep(1)  # Respectful delay
    # Clean up temp dir
    try:
        shutil.rmtree(LOCAL_TMP_DIR)
    except Exception:
        pass

if __name__ == "__main__":
    process_and_upload_london_files() 