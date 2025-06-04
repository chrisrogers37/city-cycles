from dotenv import load_dotenv
load_dotenv()
import os
import boto3
import pandas as pd
from io import BytesIO

S3_BUCKET = os.environ.get("S3_BUCKET")

s3 = boto3.client("s3")

def download_csv_from_s3(s3_key):
    csv_buffer = BytesIO()
    s3.download_fileobj(S3_BUCKET, s3_key, csv_buffer)
    csv_buffer.seek(0)
    return csv_buffer

# All database inserts are now handled by the to_database method on the data model classes. 