from dotenv import load_dotenv
load_dotenv()

import os
import boto3
import logging

S3_BUCKET = os.environ.get("S3_BUCKET")
if not S3_BUCKET:
    logging.error("S3_BUCKET environment variable is not set! Please set S3_BUCKET before running the script.")
    raise ValueError("S3_BUCKET environment variable is not set!")

private_s3 = boto3.client("s3")

def check_s3_bucket():
    if not S3_BUCKET:
        logging.error("S3_BUCKET environment variable is not set! Please set S3_BUCKET before running the script.")
        raise ValueError("S3_BUCKET environment variable is not set!")
    return S3_BUCKET

def upload_to_s3(local_path, s3_key):
    check_s3_bucket()
    print(f"Uploading CSV: {local_path} to s3://{S3_BUCKET}/{s3_key} ...")
    private_s3.upload_file(local_path, S3_BUCKET, s3_key) 