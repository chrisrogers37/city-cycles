import os
import sys
import argparse
from data_models.base import BaseBikeShareRecord
from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord

def get_model_class(model_name):
    """Get the model class from its name."""
    model_map = {
        'london_legacy': LondonLegacyBikeShareRecord,
        'london_modern': LondonModernBikeShareRecord,
        'nyc_legacy': NYCLegacyBikeShareRecord,
        'nyc_modern': NYCModernBikeShareRecord
    }
    return model_map.get(model_name.lower())

def main():
    parser = argparse.ArgumentParser(description='Load bike share data from S3 into database')
    parser.add_argument('s3_prefix', help='S3 prefix to load files from')
    parser.add_argument('--filename', help='Optional specific filename to load')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without making changes')
    parser.add_argument('--model', help='Specific model to use (london_legacy, london_modern, nyc_legacy, nyc_modern)')
    parser.add_argument('--truncate', action='store_true', help='Truncate the table before loading')
    parser.add_argument('--reload', action='store_true', help='Delete and reload the specified file')
    parser.add_argument('--chunksize', type=int, default=5000, help='Number of rows to process at once (default: 5000)')
    args = parser.parse_args()

    if args.model:
        model_class = get_model_class(args.model)
        if not model_class:
            print(f"Error: Invalid model name '{args.model}'")
            sys.exit(1)
            
        if args.truncate and not args.dry_run:
            model_class.truncate_table()
            
        if args.reload and args.filename:
            model_class.reload_file(args.filename, args.s3_prefix, args.dry_run, args.chunksize)
        else:
            model_class.load_from_s3_with_model(model_class, args.s3_prefix, args.dry_run, args.filename, args.chunksize)
    else:
        if args.truncate and not args.dry_run:
            print("Error: --truncate requires --model to be specified")
            sys.exit(1)
            
        if args.reload and args.filename:
            print("Error: --reload requires --model to be specified")
            sys.exit(1)
            
        BaseBikeShareRecord.load_from_s3(args.s3_prefix, args.dry_run, args.filename, args.chunksize)

if __name__ == '__main__':
    main() 