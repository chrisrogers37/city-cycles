import sys
from data_models.base import BaseBikeShareRecord

def main():
    if len(sys.argv) < 2:
        print("Usage: python db/batch_load_from_s3.py <s3_prefix> [<year>|<filename>] [--dry-run]")
        sys.exit(1)
    s3_prefix = sys.argv[1]
    year = None
    filename = None
    dry_run = False
    for arg in sys.argv[2:]:
        if arg.isdigit():
            year = int(arg)
        elif arg == "--dry-run":
            dry_run = True
        elif arg.endswith('.csv'):
            filename = arg
    BaseBikeShareRecord.load_from_s3(prefix=s3_prefix, year=year, filename=filename, dry_run=dry_run)

if __name__ == "__main__":
    main() 