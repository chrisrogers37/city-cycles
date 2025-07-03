#!/usr/bin/env python3
"""
CLI for Extracted File Manager
"""

import sys
import argparse
from typing import Optional
from .manager import ExtractedFileManager
from .models import FileStatus, FileType


def main():
    parser = argparse.ArgumentParser(description="Extracted File Manager CLI")
    parser.add_argument("command", choices=[
        "scan", "validate", "convert-zip", "convert-csv", "pipeline", "list", "summary", "reprocess"
    ], help="Command to execute")
    
    parser.add_argument("--file", help="Specific filename to process")
    parser.add_argument("--type", choices=["nyc_zip", "nyc_csv", "london_csv"], 
                       help="File type filter")
    parser.add_argument("--status", choices=["extracted", "csv_converted", "validated", "parquet_converted", "processed", "failed"], 
                       help="Status filter")
    parser.add_argument("--limit", type=int, help="Limit number of results")
    parser.add_argument("--batch-size", type=int, default=5, help="Number of files to process in batch (default: 5)")
    parser.add_argument("--single-file", action="store_true", help="Process only one file at a time")
    
    args = parser.parse_args()
    
    try:
        manager = ExtractedFileManager()
        
        if args.command == "scan":
            print("Scanning S3 for new files...")
            new_files = manager.scan_s3_files()
            print(f"Found {len(new_files)} new files")
            
        elif args.command == "validate":
            if args.file:
                success = manager.validate_file_schema(args.file)
                print(f"Validation {'successful' if success else 'failed'} for {args.file}")
            else:
                file_type = FileType(args.type) if args.type else None
                results = manager.validate_all_files(file_type)
                print(f"Validated {len(results)} files")
                for filename, success in results.items():
                    print(f"  {filename}: {'✓' if success else '✗'}")
                    
        elif args.command == "convert-zip":
            if not args.file:
                print("Error: --file required for convert-zip")
                sys.exit(1)
            success = manager.convert_zip_to_csv(args.file)
            print(f"ZIP to CSV conversion {'successful' if success else 'failed'} for {args.file}")
            
        elif args.command == "convert-csv":
            if not args.file:
                print("Error: --file required for convert-csv")
                sys.exit(1)
            success = manager.convert_csv_to_parquet(args.file)
            print(f"CSV to Parquet conversion {'successful' if success else 'failed'} for {args.file}")
            
        elif args.command == "pipeline":
            if args.file:
                success = manager.process_single_file(args.file)
                print(f"Pipeline {'completed' if success else 'failed'} for {args.file}")
            elif args.single_file:
                # Process just one file that needs processing
                files_to_process = [
                    filename for filename, meta in manager._metadata_cache.items()
                    if ((meta.file_type == FileType.NYC_ZIP and meta.status == FileStatus.EXTRACTED) or
                        (meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and meta.status == FileStatus.VALIDATED))
                ]
                if files_to_process:
                    filename = files_to_process[0]
                    success = manager.process_single_file(filename)
                    print(f"Pipeline {'completed' if success else 'failed'} for {filename}")
                else:
                    print("No files need processing")
            else:
                file_type = FileType(args.type) if args.type else None
                if args.batch_size == 1:
                    # Process one file at a time
                    results = manager.process_files_batch(file_type, limit=1)
                else:
                    # Process in batches
                    results = manager.process_files_batch(file_type, limit=args.batch_size)
                print(f"Processed {len(results)} files through pipeline")
                for filename, success in results.items():
                    print(f"  {filename}: {'✓' if success else '✗'}")
                    
        elif args.command == "list":
            status = FileStatus(args.status) if args.status else None
            file_type = FileType(args.type) if args.type else None
            
            files = manager.list_files(status=status, file_type=file_type, limit=args.limit)
            print(f"Found {len(files)} files:")
            for file_meta in files:
                print(f"  {file_meta.filename} ({file_meta.file_type.value}) - {file_meta.status.value}")
                
        elif args.command == "summary":
            manager.print_summary()
            
        elif args.command == "reprocess":
            if not args.file:
                print("Error: --file required for reprocess")
                sys.exit(1)
            success = manager.reprocess_file(args.file)
            print(f"Reprocess {'successful' if success else 'failed'} for {args.file}")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 