#!/usr/bin/env python3
"""
Simplified CLI for Extracted File Manager

Replaces the complex CLI with simple pipeline commands that are idempotent
and don't require complex metadata management.
"""

import sys
import argparse
from .simplified_pipeline import (
    run_full_pipeline, 
    run_extraction_only, 
    run_conversion_only
)

def main():
    parser = argparse.ArgumentParser(
        description="Simplified Extracted File Manager CLI",
        epilog="""
Examples:
  python -m extracted_file_manager.cli run          # Run full pipeline
  python -m extracted_file_manager.cli extract      # Extract files only
  python -m extracted_file_manager.cli convert      # Convert files only
        """
    )
    
    parser.add_argument("command", choices=[
        "run", "extract", "convert"
    ], help="Pipeline command to execute")
    
    args = parser.parse_args()
    
    try:
        if args.command == "run":
            print("Running complete pipeline...")
            run_full_pipeline()
            
        elif args.command == "extract":
            print("Running extraction phase only...")
            run_extraction_only()
            
        elif args.command == "convert":
            print("Running conversion phase only...")
            run_conversion_only()
            
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
