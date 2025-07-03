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
        "scan", "validate", "extract_zips", "convert_csvs", "pipeline", "list", "summary", "reprocess",
        "wipe-file", "wipe-type", "wipe-all", "reprocess-failed", "reset-failed", "cleanup-macos-files",
        "set-schema", "clear-schema", "fix-parquet-status"
    ], help="Command to execute")
    
    parser.add_argument("--file", help="Specific filename to process")
    parser.add_argument("--type", choices=["nyc_zip", "nyc_csv", "london_csv", "nyc_parquet", "london_parquet"], 
                       help="File type filter")
    parser.add_argument("--location", choices=["nyc", "london"], help="Location filter for wipe operations")
    parser.add_argument("--schema", help="Schema filter for parquet wipe operations or schema name for override")
    parser.add_argument("--status", choices=["extracted", "csv_converted", "validated", "parquet_converted", "processed", "failed"], 
                       help="Status filter")
    parser.add_argument("--limit", type=int, help="Limit number of results")
    parser.add_argument("--confirm", action="store_true", help="Confirm destructive operations")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging for validation failures")
    
    args = parser.parse_args()
    
    # Set debug mode globally if requested
    if args.debug:
        import os
        os.environ['EXTRACTED_FILE_MANAGER_DEBUG'] = '1'
    
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
                    
        elif args.command == "extract_zips":
            count = manager.extract_all_zips(limit=args.limit)
            print(f"Extracted {count} ZIP files to CSVs.")
        
        elif args.command == "convert_csvs":
            count = manager.convert_all_csvs(limit=args.limit)
            print(f"Validated and converted {count} CSV files to Parquet.")
        
        elif args.command == "pipeline":
            print("Running full pipeline: extract_zips then convert_csvs...")
            count_zips = manager.extract_all_zips(limit=args.limit)
            count_csvs = manager.convert_all_csvs(limit=args.limit)
            print(f"Pipeline complete. Extracted {count_zips} ZIPs, converted {count_csvs} CSVs.")
        
        elif args.command == "wipe-file":
            if not args.file:
                print("Error: --file required for wipe-file")
                sys.exit(1)
            if not args.confirm:
                print(f"Error: --confirm required for destructive operation: wipe-file {args.file}")
                sys.exit(1)
            success = manager.wipe_file(args.file)
            print(f"Wipe {'successful' if success else 'failed'} for {args.file}")
        
        elif args.command == "wipe-type":
            if not args.type:
                print("Error: --type required for wipe-type")
                sys.exit(1)
            if not args.confirm:
                print(f"Error: --confirm required for destructive operation: wipe-type {args.type}")
                sys.exit(1)
            count = manager.wipe_file_type(args.type, args.location, args.schema)
            print(f"Wiped {count} files of type {args.type}")
        
        elif args.command == "wipe-all":
            if not args.confirm:
                print("Error: --confirm required for destructive operation: wipe-all")
                sys.exit(1)
            count = manager.wipe_all()
            print(f"Wiped {count} files total")
        
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
            
        elif args.command == "reset-failed":
            city = args.location
            file_type = FileType(args.type) if args.type else None
            yes = args.confirm
            
            failed_files = manager.list_failed_files(city=city, file_type=file_type)
            
            if not failed_files:
                print("No failed files found to reset.")
                return
            
            print(f"Found {len(failed_files)} failed files to reset:")
            for file_info in failed_files:
                print(f"  - {file_info['key']}")
            
            if not yes:
                confirm = input(f"\nReset {len(failed_files)} failed files to 'extracted' status? (y/N): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    return
            
            reset_count = manager.reset_failed_files(city=city, file_type=file_type)
            print(f"Successfully reset {reset_count} failed files to 'extracted' status.")
            
        elif args.command == "reprocess-failed":
            city = args.location
            file_type = FileType(args.type) if args.type else None
            yes = args.confirm
            
            failed_files = manager.list_failed_files(city=city, file_type=file_type)
            
            if not failed_files:
                print("No failed files found to reprocess.")
                return
            
            print(f"Found {len(failed_files)} failed files to reprocess:")
            for file_info in failed_files:
                print(f"  - {file_info['key']}")
            
            if not yes:
                confirm = input(f"\nReset and reprocess {len(failed_files)} failed files? (y/N): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    return
            
            # Reset failed files
            reset_count = manager.reset_failed_files(city=city, file_type=file_type)
            print(f"Reset {reset_count} failed files to 'extracted' status.")
            
            # Reprocess the files
            print("Starting reprocessing...")
            manager.run_pipeline(city=city, file_type=file_type)
            print("Reprocessing completed.")
            
        elif args.command == "cleanup-macos-files":
            if not args.confirm:
                print("Error: --confirm required for cleanup-macos-files")
                sys.exit(1)
            
            # Find all Mac OS hidden files in metadata
            macos_files = []
            for filename, meta in manager._metadata_cache.items():
                if filename.startswith('._'):
                    macos_files.append((filename, meta))
            
            if not macos_files:
                print("No Mac OS hidden files found in metadata.")
                return
            
            print(f"Found {len(macos_files)} Mac OS hidden files in metadata:")
            for filename, meta in macos_files:
                print(f"  - {filename} ({meta.s3_key})")
            
            # Remove from metadata
            removed_count = 0
            for filename, meta in macos_files:
                try:
                    # Try to delete from S3 (will fail if file doesn't exist, but that's OK)
                    try:
                        manager.s3_client.delete_object(Bucket=manager.s3_bucket, Key=meta.s3_key)
                        print(f"Deleted from S3: {meta.s3_key}")
                    except Exception as e:
                        print(f"File not found in S3 (expected): {meta.s3_key}")
                    
                    # Remove from metadata
                    del manager._metadata_cache[filename]
                    removed_count += 1
                    print(f"Removed from metadata: {filename}")
                    
                except Exception as e:
                    print(f"Error removing {filename}: {e}")
            
            # Save updated metadata
            if removed_count > 0:
                manager._save_metadata()
                print(f"\nSuccessfully removed {removed_count} Mac OS hidden files from metadata.")
            else:
                print("\nNo files were removed.")
            
        elif args.command == "set-schema":
            if not args.file:
                print("Error: --file required for set-schema")
                sys.exit(1)
            if not args.schema:
                print("Error: --schema required for set-schema")
                sys.exit(1)
            success = manager.set_schema_override(args.file, args.schema)
            print(f"Schema {'successfully' if success else 'failed'} set for {args.file}")
        
        elif args.command == "clear-schema":
            if not args.file:
                print("Error: --file required for clear-schema")
                sys.exit(1)
            success = manager.clear_schema_override(args.file)
            print(f"Schema {'successfully' if success else 'failed'} cleared for {args.file}")
            
        elif args.command == "fix-parquet-status":
            city = args.location
            confirm = args.confirm
            
            # Find CSV files with parquet_converted status
            csv_files_to_reset = []
            for filename, meta in manager._metadata_cache.items():
                if (meta.file_type in [FileType.NYC_CSV, FileType.LONDON_CSV] and 
                    meta.status == FileStatus.PARQUET_CONVERTED):
                    
                    # Apply city filter
                    if city and city not in meta.s3_key:
                        continue
                    
                    csv_files_to_reset.append(meta)
            
            if not csv_files_to_reset:
                print("No CSV files with parquet_converted status found.")
                return
            
            print(f"Found {len(csv_files_to_reset)} CSV files with parquet_converted status:")
            for meta in csv_files_to_reset[:10]:  # Show first 10
                print(f"  - {meta.filename}")
            if len(csv_files_to_reset) > 10:
                print(f"  ... and {len(csv_files_to_reset) - 10} more")
            
            if not confirm:
                confirm_input = input(f"\nReset these {len(csv_files_to_reset)} CSV files to EXTRACTED status? (y/N): ")
                if confirm_input.lower() != 'y':
                    print("Operation cancelled.")
                    return
            
            # Reset the files
            reset_count = 0
            for meta in csv_files_to_reset:
                meta.status = FileStatus.EXTRACTED
                meta.parquet_converted_at = None
                meta.parquet_s3_key = None
                meta.parquet_schema = None
                reset_count += 1
            
            manager._save_metadata()
            print(f"Reset {reset_count} CSV files to EXTRACTED status.")
            print("You can now run 'convert_csvs' to reconvert them to Parquet.")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 