from data_models.base import BaseBikeShareRecord

PREFIXES = ["london_csv/", "nyc_csv/"]

def main():
    for prefix in PREFIXES:
        print(f"\n--- Processing files in {prefix} ---")
        try:
            BaseBikeShareRecord.load_from_s3(prefix=prefix)
        except Exception as e:
            print(f"[ERROR] Failed to load files for prefix {prefix}: {e}")

if __name__ == "__main__":
    main() 