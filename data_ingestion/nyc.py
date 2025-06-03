import os
import re
import boto3
from datetime import datetime
import zipfile

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
    print(f"Uploading {local_path} to s3://{S3_BUCKET}/{s3_key} ...")
    private_s3.upload_file(local_path, S3_BUCKET, s3_key)

def unzip_and_upload_csvs(local_zip_path, s3_prefix):
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            if member.endswith('.csv'):
                extracted_path = zip_ref.extract(member, LOCAL_TMP_DIR)
                s3_key = f"{s3_prefix}/{os.path.basename(member)}"
                upload_to_s3(extracted_path, s3_key)
                os.remove(extracted_path)
                print(f"Uploaded and cleaned up {member}")

def download_unzip_upload_all(start_year=2018, end_year=None):
    print(f"Using S3 bucket: {S3_BUCKET}")
    files = list_nyc_citibike_files(start_year, end_year)
    print(f"Found {len(files)} files to process.")
    for key in files:
        fname = os.path.basename(key)
        local_path = os.path.join(LOCAL_TMP_DIR, fname)
        # Download zip
        download_file_from_s3(NYC_PUBLIC_BUCKET, key, local_path)
        # Unzip and upload CSVs
        unzip_and_upload_csvs(local_path, "nyc_csv")
        # Remove zip
        os.remove(local_path)
        print(f"Done: {fname}")

if __name__ == "__main__":
    download_unzip_upload_all() 