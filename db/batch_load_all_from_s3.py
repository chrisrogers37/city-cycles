from data_models.base import BaseBikeShareRecord

PREFIXES = ["london_csv/", "nyc_csv/"]

def main():
    for prefix in PREFIXES:
        print(f"\n--- Processing files in {prefix} ---")
        BaseBikeShareRecord.load_from_s3(prefix=prefix)

if __name__ == "__main__":
    main() 