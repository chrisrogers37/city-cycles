#!/usr/bin/env python3
"""
CLI for Extracted File Manager
"""

import sys
import argparse
from typing import Optional
from .manager import ExtractedFileManager
from .models import FileStatus, FileType, FileLocation


def main():
    parser = argparse.ArgumentParser(description="Extracted File Manager CLI")
    parser.add_argument("command", choices=[
        "scan", "validate", "extract_zips", "convert_csvs", "list", "summary", "reprocess",
        "wipe-file", "wipe-type", "wipe-all", "reset-failed", "set-schema"
    ], help="Command to execute")
    
    parser.add_argument("--file", help="Specific filename to process")
    parser.add_argument("--file-type", choices=["zip", "csv", "parquet"], 
                       help="File type filter")
    parser.add_argument("--location", choices=["nyc", "london"], help="Location filter")
    parser.add_argument("--schema", help="Schema filter for parquet wipe operations or schema name for override")
    parser.add_argument("--status", choices=["extracted", "csv_converted", "validated", "parquet_converted", "processed", "failed"], 
                       help="Status filter")
    parser.add_argument("--confirm", action="store_true", help="Confirm destructive operations")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging for validation failures")
    parser.add_argument("--reprocess", action="store_true", help="Reprocess files after reset (for reset-failed command)")
    parser.add_argument("--clear", action="store_true", help="Clear schema override (for set-schema command)")
    
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
                file_type = FileType(args.file_type) if args.file_type else None
                results = manager.validate_all_files(file_type)
                print(f"Validated {len(results)} files")
                for filename, success in results.items():
                    print(f"  {filename}: {'✓' if success else '✗'}")
                    
        elif args.command == "extract_zips":
            # Handle single file or all files
            if args.file:
                results = manager.extract_zips(filenames=[args.file])
                success_count = sum(1 for success in results.values() if success)
                print(f"Extracted {success_count}/{len(results)} ZIP files to CSVs.")
            else:
                location = FileLocation(args.location) if args.location else None
                results = manager.extract_zips(location=location)
                success_count = sum(1 for success in results.values() if success)
                print(f"Extracted {success_count}/{len(results)} ZIP files to CSVs.")
        
        elif args.command == "convert_csvs":
            # Handle single file or all files
            if args.file:
                results = manager.convert_csvs(filenames=[args.file])
                success_count = sum(1 for success in results.values() if success)
                print(f"Converted {success_count}/{len(results)} CSV files to Parquet.")
            else:
                location = FileLocation(args.location) if args.location else None
                results = manager.convert_csvs(location=location)
                success_count = sum(1 for success in results.values() if success)
                print(f"Converted {success_count}/{len(results)} CSV files to Parquet.")
        
        elif args.command == "wipe-file":
            if not args.file:
                print("Error: --file required for wipe-file")
                sys.exit(1)
            if not args.confirm:
                print(f"Error: --confirm required for destructive operation: wipe-file {args.file}")
                sys.exit(1)
            count = manager.wipe_files(filenames=[args.file])
            print(f"Wiped {count} file(s)")
        
        elif args.command == "wipe-type":
            if not args.file_type:
                print("Error: --file-type required for wipe-type")
                sys.exit(1)
            if not args.confirm:
                print(f"Error: --confirm required for destructive operation: wipe-type {args.file_type}")
                sys.exit(1)
            file_type = FileType(args.file_type)
            location = FileLocation(args.location) if args.location else None
            count = manager.wipe_files(file_type=file_type, location=location)
            print(f"Wiped {count} files of type {args.file_type}")
        
        elif args.command == "wipe-all":
            if not args.confirm:
                print("Error: --confirm required for destructive operation: wipe-all")
                sys.exit(1)
            print("WARNING: This will delete ALL files (ZIPs, CSVs, Parquets) from S3 and remove ALL metadata!")
            count = manager.wipe_files()
            print(f"Wiped {count} files total")
        
        elif args.command == "list":
            status = FileStatus(args.status) if args.status else None
            file_type = FileType(args.file_type) if args.file_type else None
            
            files = manager.list_files(status=status, file_type=file_type)
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
            file_type = FileType(args.file_type) if args.file_type else None
            yes = args.confirm
            reprocess = args.reprocess
            
            failed_files = manager.list_failed_files(city=city, file_type=file_type)
            
            if not failed_files:
                print("No failed files found to reset.")
                return
            
            action = "reset and reprocess" if reprocess else "reset"
            print(f"Found {len(failed_files)} failed files to {action}:")
            for file_info in failed_files:
                print(f"  - {file_info['key']}")
            
            if not yes:
                confirm = input(f"\n{action.title()} {len(failed_files)} failed files? (y/N): ")
                if confirm.lower() != 'y':
                    print("Operation cancelled.")
                    return
            
            reset_count = manager.reset_failed_files(city=city, file_type=file_type)
            print(f"Successfully reset {reset_count} failed files to 'extracted' status.")
            
            if reprocess:
                print("Files have been reset to 'extracted' status.")
                print("Run 'extract_zips' or 'convert_csvs' to process them.")
            
        elif args.command == "set-schema":
            if not args.file:
                print("Error: --file required for set-schema")
                sys.exit(1)
            
            if args.clear:
                # Clear schema override
                success = manager.clear_schema_override(args.file)
                print(f"Schema {'successfully' if success else 'failed'} cleared for {args.file}")
            else:
                # Set schema override
                if not args.schema:
                    print("Error: --schema required for set-schema (or use --clear to remove)")
                    sys.exit(1)
                success = manager.set_schema_override(args.file, args.schema)
                print(f"Schema {'successfully' if success else 'failed'} set for {args.file}")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 