"""
Simplified Pipeline for Bike Share Data Processing

Provides simple, idempotent functions for running the complete data pipeline
from extraction to parquet conversion without complex metadata management.
"""

import os
from typing import Dict, Any
from .manager import ExtractedFileManager
from extraction.nyc import download_all_zips
from extraction.london import process_and_upload_london_files

def run_extraction_phase() -> Dict[str, Any]:
    """
    Step 1: Extract all files from web sources to S3.
    
    This phase downloads ZIP files from NYC CitiBike and CSV files from London TfL,
    storing them in S3. The extraction functions already check for existing files
    and skip downloads if files already exist.
    
    Returns:
        Dict with extraction results
    """
    print("=== EXTRACTION PHASE ===")
    
    results = {}
    
    # NYC: Download ZIPs (already checks if they exist)
    print("Extracting NYC data...")
    try:
        download_all_zips()
        results["nyc"] = "success"
        print("✓ NYC extraction complete")
    except Exception as e:
        results["nyc"] = f"failed: {e}"
        print(f"✗ NYC extraction failed: {e}")
    
    # London: Download CSVs (already checks if they exist)  
    print("Extracting London data...")
    try:
        process_and_upload_london_files()
        results["london"] = "success"
        print("✓ London extraction complete")
    except Exception as e:
        results["london"] = f"failed: {e}"
        print(f"✗ London extraction failed: {e}")
    
    print("✓ Extraction phase complete")
    return results

def run_conversion_phase() -> Dict[str, Dict[str, bool]]:
    """
    Step 2: Convert all raw files to parquet with proper organization.
    
    This phase:
    1. Extracts all ZIPs to CSVs (skips if CSVs already exist)
    2. Converts all CSVs to parquet (skips if parquet already exists)
    3. Organizes parquet files by schema for downstream analytics
    
    Returns:
        Dict with conversion results for both ZIP extraction and CSV conversion
    """
    print("=== CONVERSION PHASE ===")
    
    # Initialize the simplified manager
    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        raise ValueError("S3_BUCKET environment variable required")
    
    manager = ExtractedFileManager(s3_bucket)
    
    # Run the complete conversion pipeline
    results = manager.run_simplified_pipeline()
    
    print("✓ Conversion phase complete")
    return results

def run_full_pipeline() -> Dict[str, Any]:
    """
    Run the complete end-to-end pipeline.
    
    This function runs both extraction and conversion phases in sequence.
    The pipeline is fully idempotent - running it multiple times will only
    process new files and skip files that have already been processed.
    
    Returns:
        Dict with results from both phases
    """
    print("🚀 Starting City Cycles Data Pipeline")
    print("=" * 50)
    
    # Phase 1: Extraction
    extraction_results = run_extraction_phase()
    
    # Phase 2: Conversion
    conversion_results = run_conversion_phase()
    
    # Summary
    print("\n" + "=" * 50)
    print("🎉 PIPELINE COMPLETE")
    print("=" * 50)
    
    # Print extraction summary
    print("\nExtraction Results:")
    for source, result in extraction_results.items():
        status = "✓" if result == "success" else "✗"
        print(f"  {status} {source.upper()}: {result}")
    
    # Print conversion summary
    print("\nConversion Results:")
    if "extraction" in conversion_results:
        extraction_success = sum(1 for success in conversion_results["extraction"].values() if success)
        extraction_total = len(conversion_results["extraction"])
        print(f"  ZIP Extraction: {extraction_success}/{extraction_total} successful")
    
    if "conversion" in conversion_results:
        conversion_success = sum(1 for success in conversion_results["conversion"].values() if success)
        conversion_total = len(conversion_results["conversion"])
        print(f"  CSV Conversion: {conversion_success}/{conversion_total} successful")
    
    print("\nPipeline is idempotent - you can run it again safely!")
    
    return {
        "extraction": extraction_results,
        "conversion": conversion_results
    }

def run_extraction_only() -> Dict[str, Any]:
    """
    Run only the extraction phase.
    
    Useful when you only want to download new files without processing them.
    
    Returns:
        Dict with extraction results
    """
    return run_extraction_phase()

def run_conversion_only() -> Dict[str, Dict[str, bool]]:
    """
    Run only the conversion phase.
    
    Useful when you have files already extracted and only want to convert them.
    
    Returns:
        Dict with conversion results
    """
    return run_conversion_phase()

if __name__ == "__main__":
    # When run as a script, execute the full pipeline
    run_full_pipeline()
