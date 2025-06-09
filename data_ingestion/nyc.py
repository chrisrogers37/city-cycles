from dotenv import load_dotenv
load_dotenv()
import os
import re
import boto3
from datetime import datetime
import zipfile
import shutil
from data_ingestion.utils import upload_to_s3, file_exists_in_s3

S3_BUCKET = os.environ.get("S3_BUCKET")
NYC_PUBLIC_BUCKET = "tripdata"
LOCAL_TMP_DIR = "/tmp/extracted_bike_ride_zips/nyc/"
RAW_ZIP_PREFIX = "extracted_bike_ride_zips/nyc"

# Ensure local temp dir exists
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# Use unsigned config for public S3 bucket
from botocore import UNSIGNED
from botocore.client import Config
public_s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

def list_nyc_citibike_files(start_year=2019, end_year=None):
    if end_year is None:
        end_year = datetime.now().year
    print(f"Listing files in s3://{NYC_PUBLIC_BUCKET}/ ...")
    paginator = public_s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=NYC_PUBLIC_BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Only .zip files
            if not key.endswith(".zip"):
                continue
            # Extract year from filename
            match = re.match(r"(\d{4})", os.path.basename(key))
            if match:
                year = int(match.group(1))
                if start_year <= year <= end_year:
                    files.append(key)
    print(f"Matched {len(files)} files for years {start_year}-{end_year}.")
    print(f"Sample files: {files[:5]}")
    return files

def download_file_from_s3(bucket, key, dest_path):
    print(f"Downloading s3://{bucket}/{key} to {dest_path} ...")
    public_s3.download_file(bucket, key, dest_path)

def is_valid_zip(path):
    try:
        with zipfile.ZipFile(path, 'r') as zf:
            bad_file = zf.testzip()
            return bad_file is None
    except zipfile.BadZipFile:
        return False

def download_and_store_zip(key):
    """
    Download a ZIP file and store it in the raw_zip_files folder in S3.
    Returns True if the file was downloaded and stored, False if it already exists.
    """
    fname = os.path.basename(key)
    local_path = os.path.join(LOCAL_TMP_DIR, fname)
    s3_key = f"{RAW_ZIP_PREFIX}/{fname}"
    
    # Check if we already have this ZIP file
    if file_exists_in_s3(s3_key):
        print(f"ZIP file already exists in S3: {s3_key}")
        return False
        
    try:
        # Download the ZIP file
        download_file_from_s3(NYC_PUBLIC_BUCKET, key, local_path)
        
        # Validate the ZIP file
        if not is_valid_zip(local_path):
            print(f"ERROR: Invalid ZIP file: {local_path}")
            os.remove(local_path)
            return False
            
        # Upload to our S3 bucket
        upload_to_s3(local_path, s3_key)
        print(f"Stored ZIP file in S3: {s3_key}")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to process {key}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)

def download_all_zips(start_year=2019, end_year=None):
    """
    Download all ZIP files from NYC CitiBike and store them in S3.
    """
    print(f"Using S3 bucket: {S3_BUCKET}")
    files = list_nyc_citibike_files(start_year, end_year)
    print(f"Found {len(files)} files to process.")
    
    downloaded_count = 0
    skipped_count = 0
    
    for key in files:
        if download_and_store_zip(key):
            downloaded_count += 1
        else:
            skipped_count += 1
            
    print(f"\nDownload Summary:")
    print(f"Total files found: {len(files)}")
    print(f"New files downloaded: {downloaded_count}")
    print(f"Files already in S3: {skipped_count}")

if __name__ == "__main__":
    download_all_zips() 