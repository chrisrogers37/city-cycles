import os
import boto3

S3_BUCKET = os.environ.get("S3_BUCKET")
private_s3 = boto3.client("s3")

def upload_to_s3(local_path, s3_key):
    print(f"Uploading CSV: {local_path} to s3://{S3_BUCKET}/{s3_key} ...")
    private_s3.upload_file(local_path, S3_BUCKET, s3_key) 