import os
import sys
from data_models.base import BaseBikeShareRecord

NYC_PREFIX = "nyc_csv/"
CUTOFF_FILENAME = "202302-citibike-tripdata_1.csv"

def main():
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == "--run":
        dry_run = False
    files = BaseBikeShareRecord.list_s3_files(prefix=NYC_PREFIX)
    files = sorted([os.path.basename(f) for f in files])
    try:
        cutoff_idx = files.index(CUTOFF_FILENAME)
    except ValueError:
        print(f"Cutoff file {CUTOFF_FILENAME} not found in S3 listing.")
        return
    files_to_process = files[cutoff_idx:]
    print("Files to process:")
    for f in files_to_process:
        print(f)
    if dry_run:
        print("\n(DRY RUN: No files will be loaded)")
        return
    for f in files_to_process:
        print(f"\nProcessing {f} ...")
        os.system(f"python -m db.batch_load_from_s3 nyc_csv/ {f}")

if __name__ == "__main__":
    main() 