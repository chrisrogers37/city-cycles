import os
import re
import boto3
from datetime import datetime
import zipfile
import shutil

S3_BUCKET = os.environ.get("S3_BUCKET")
NYC_PUBLIC_BUCKET = "tripdata"
LOCAL_TMP_DIR = "/tmp/nyc_citibike/"

# Ensure local temp dir exists
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# Use unsigned config for public S3 bucket
from botocore import UNSIGNED
from botocore.client import Config
public_s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
private_s3 = boto3.client("s3")

def list_nyc_citibike_files(start_year=2018, end_year=None):
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

def upload_to_s3(local_path, s3_key):
    print(f"Uploading CSV: {local_path} to s3://{S3_BUCKET}/{s3_key} ...")
    private_s3.upload_file(local_path, S3_BUCKET, s3_key)

def is_valid_zip(path):
    try:
        with zipfile.ZipFile(path, 'r') as zf:
            bad_file = zf.testzip()
            return bad_file is None
    except zipfile.BadZipFile:
        return False

def process_path(path, s3_prefix):
    """
    Recursively process a file or directory:
    - If CSV, upload to S3
    - If zip, extract and process contents
    - If directory, process contents
    """
    try:
        if os.path.isdir(path):
            print(f"[DIR] {path}")
            for entry in os.listdir(path):
                process_path(os.path.join(path, entry), s3_prefix)
            # Clean up directory after processing
            if path != LOCAL_TMP_DIR:
                shutil.rmtree(path)
        elif path.endswith('.csv'):
            print(f"[CSV] {path}")
            s3_key = f"{s3_prefix}/{os.path.basename(path)}"
            upload_to_s3(path, s3_key)
            os.remove(path)
            print(f"Uploaded and cleaned up CSV: {path}")
        elif path.endswith('.zip'):
            print(f"[ZIP] {path}")
            if is_valid_zip(path):
                extract_dir = path + "_extracted"
                os.makedirs(extract_dir, exist_ok=True)
                try:
                    with zipfile.ZipFile(path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                        print(f"Extracted zip: {path} to {extract_dir}")
                except Exception as e:
                    print(f"ERROR: Failed to extract zip {path}: {e}")
                    os.remove(path)
                    return
                os.remove(path)
                process_path(extract_dir, s3_prefix)
            else:
                print(f"ERROR: Skipping invalid zip file: {path}")
                os.remove(path)
        else:
            print(f"[SKIP] Unsupported file: {path}")
            os.remove(path)
    except Exception as e:
        print(f"ERROR: Unexpected error processing {path}: {e}")
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

def download_unzip_upload_all(start_year=2019, end_year=None):
    print(f"Using S3 bucket: {S3_BUCKET}")
    files = list_nyc_citibike_files(start_year, end_year)
    print(f"Found {len(files)} files to process.")
    for key in files:
        fname = os.path.basename(key)
        local_path = os.path.join(LOCAL_TMP_DIR, fname)
        try:
            download_file_from_s3(NYC_PUBLIC_BUCKET, key, local_path)
            try:
                process_path(local_path, "nyc_csv")
            except Exception as e:
                print(f"ERROR: Failed to process {local_path}: {e}")
        except Exception as e:
            print(f"ERROR: Failed to download or process {key}: {e}")
        print(f"Done: {fname}")

if __name__ == "__main__":
    download_unzip_upload_all() 