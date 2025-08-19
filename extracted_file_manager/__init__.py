"""
Simplified Extracted File Manager

A streamlined file management system for processing bike share data from ZIP archives 
through CSV extraction to optimized Parquet storage.

This package uses simple file existence checks instead of complex metadata tracking
for better reliability and easier debugging.
"""

from .manager import ExtractedFileManager
from .simplified_pipeline import run_full_pipeline, run_extraction_only, run_conversion_only

__all__ = [
    'ExtractedFileManager',
    'run_full_pipeline',
    'run_extraction_only', 
    'run_conversion_only'
] 