# Phase 08: Orchestrator & Extraction Cleanup

**PR Title:** `refactor(orchestrator): consolidate logging patterns, replace prints with logger, narrow exception handling`
**Risk Level:** Low
**Estimated Effort:** Medium (3-4 hours)
**Dependencies:** None (touches disjoint files from all other phases)
**Blocks:** Phase 09 (test coverage expansion)

---

## Summary

Clean up the orchestrator and extraction modules by: (1) consolidating the 26+ repeated `logger.info("=" * 80)` separator patterns in `orchestrator/main.py` into helper functions, (2) replacing `print()` calls with `logger` in `orchestrator/cli.py`, (3) narrowing bare `except Exception` catches in `extraction/nyc.py` and `extraction/london.py` to specific exception types, and (4) replacing `print()` with `logging` in `extraction/utils.py`. No functional behavior changes -- only observability and code hygiene improvements.

---

## Files Modified

| # | File | Action |
|---|------|--------|
| 1 | `orchestrator/main.py` | Add logging helpers, replace 26 separator patterns |
| 2 | `orchestrator/cli.py` | Replace `print()` with `logger` in `check_pipeline_status` |
| 3 | `extraction/nyc.py` | Narrow exception handling, replace `print()` with `logger` |
| 4 | `extraction/london.py` | Narrow exception handling, replace `print()` with `logger` |
| 5 | `extraction/utils.py` | Replace `print()` with `logger` |

---

## Change 1: Add Logging Helpers to `orchestrator/main.py`

**Problem:** The pattern of `logger.info("=" * 80)` followed by a header message, followed by another `logger.info("=" * 80)` appears 10 times in `orchestrator/main.py`. Each pipeline step and the success/failure reports all use this 3-line pattern. The repetition makes the code harder to scan and increases the line count by ~20 lines unnecessarily.

**Locations of the pattern (all in `orchestrator/main.py`):**
- Lines 70-72: Pipeline start
- Lines 114-116: Step 1 header
- Lines 157-159: Step 2 header
- Lines 182-184: Step 3 header
- Lines 212-214: Step 4 header
- Lines 294-296: Step 5 header
- Lines 316-318: Success report
- Lines 330 (line 330 only uses `"=" * 80`): Success report end
- Lines 336-338: Failure report
- Lines 352 (line 352 only): Failure report end

### Step 1a: Add helper functions

Add these two helper functions immediately after the `logger = logging.getLogger(__name__)` line (after line 22) and before the `class CityBikesOrchestrator:` definition (line 25):

**BEFORE (lines 22-25):**
```python
logger = logging.getLogger(__name__)


class CityBikesOrchestrator:
```

**AFTER (lines 22-42):**
```python
logger = logging.getLogger(__name__)


def _log_section(title: str, level: str = "info", width: int = 80):
    """Log a section header with separator lines above and below.

    Args:
        title: The header text to display
        level: Logging level ('info' or 'error')
        width: Width of the separator line in characters
    """
    log_fn = getattr(logger, level)
    log_fn("=" * width)
    log_fn(title)
    log_fn("=" * width)


def _log_step(step: int, total: int, description: str):
    """Log a pipeline step header with separator lines.

    Args:
        step: Current step number (1-indexed)
        total: Total number of steps
        description: Human-readable description of the step
    """
    logger.info("")
    _log_section(f"[STEP {step}/{total}] {description}")


class CityBikesOrchestrator:
```

### Step 1b: Replace all separator patterns

**Each replacement below is independent. Apply all of them.**

#### Location 1: Pipeline start (lines 70-72)

**BEFORE:**
```python
        logger.info("=" * 80)
        logger.info(f"CITY CYCLES PIPELINE - Starting at {self.pipeline_start}")
        logger.info("=" * 80)
```

**AFTER:**
```python
        _log_section(f"CITY CYCLES PIPELINE - Starting at {self.pipeline_start}")
```

#### Location 2: Step 1 -- Extraction (lines 114-116)

**BEFORE:**
```python
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 1/5] EXTRACTING BIKE DATA FROM WEB TO S3")
        logger.info("=" * 80)
```

**AFTER:**
```python
        _log_step(1, 5, "EXTRACTING BIKE DATA FROM WEB TO S3")
```

#### Location 3: Step 2 -- File management (lines 157-159)

**BEFORE:**
```python
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 2/5] PROCESSING FILES (UNZIP, SCHEMA VALIDATION, PARQUET)")
        logger.info("=" * 80)
```

**AFTER:**
```python
        _log_step(2, 5, "PROCESSING FILES (UNZIP, SCHEMA VALIDATION, PARQUET)")
```

#### Location 4: Step 3 -- Database load (lines 182-184)

**BEFORE:**
```python
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 3/5] LOADING DATA INTO DUCKDB")
        logger.info("=" * 80)
```

**AFTER:**
```python
        _log_step(3, 5, "LOADING DATA INTO DUCKDB")
```

#### Location 5: Step 4 -- dbt (lines 212-214)

**BEFORE:**
```python
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 4/5] RUNNING DBT TRANSFORMATIONS")
        logger.info("=" * 80)
```

**AFTER:**
```python
        _log_step(4, 5, "RUNNING DBT TRANSFORMATIONS")
```

#### Location 6: Step 5 -- Export (lines 294-296)

**BEFORE:**
```python
        logger.info("\n" + "=" * 80)
        logger.info("[STEP 5/5] EXPORTING DATA MARTS TO S3")
        logger.info("=" * 80)
```

**AFTER:**
```python
        _log_step(5, 5, "EXPORTING DATA MARTS TO S3")
```

#### Location 7: Success report (lines 316-318)

**BEFORE:**
```python
        logger.info("\n" + "=" * 80)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
```

**AFTER:**
```python
        logger.info("")
        _log_section("PIPELINE COMPLETED SUCCESSFULLY")
```

#### Location 8: Success report end separator (line 330)

**BEFORE:**
```python
        logger.info("=" * 80)
```

**AFTER:**
(Delete this line entirely. The section header already has separators above and below the title.)

#### Location 9: Failure report (lines 336-338)

**BEFORE:**
```python
        logger.error("\n" + "=" * 80)
        logger.error("✗ PIPELINE FAILED")
        logger.error("=" * 80)
```

**AFTER:**
```python
        logger.error("")
        _log_section("PIPELINE FAILED", level="error")
```

#### Location 10: Failure report end separator (line 352)

**BEFORE:**
```python
        logger.error("=" * 80)
```

**AFTER:**
(Delete this line entirely.)

### Lines Affected in `orchestrator/main.py`

Total: ~30 lines replaced with ~10 lines plus ~18 lines of new helper functions. Net change: reduces the file by ~10 lines while improving readability.

---

## Change 2: Replace `print()` with `logger` in `orchestrator/cli.py`

**Problem:** The `check_pipeline_status` function (lines 159-200) uses `print()` for all output instead of the `logging` module. This means status output bypasses the logging configuration (log level, format, handlers) and cannot be captured by log files or filtered by verbosity settings.

**Note:** The lines in `main()` that use `print()` (lines 152 and 155) are for fatal error and keyboard interrupt messages. These are acceptable as `print()` because they run after the program is about to exit, and logging may not be properly initialized in error states. Leave them as `print()`.

### Step 2a: Add logger at module level

A `logger` is not currently defined in `cli.py`. Add it after the imports.

**BEFORE (lines 1-12):**
```python
#!/usr/bin/env python3
"""
CLI Interface for City Cycles Pipeline Orchestrator

Provides command-line interface for running the orchestrator with various options.
"""

import sys
import argparse
import logging
from pathlib import Path
from .main import CityBikesOrchestrator
```

**AFTER (lines 1-14):**
```python
#!/usr/bin/env python3
"""
CLI Interface for City Cycles Pipeline Orchestrator

Provides command-line interface for running the orchestrator with various options.
"""

import sys
import argparse
import logging
from pathlib import Path
from .main import CityBikesOrchestrator, _log_section

logger = logging.getLogger(__name__)
```

**Why import `_log_section`:** The `check_pipeline_status` function uses the same `"=" * 80` separator pattern. We can reuse the helper from `main.py` instead of duplicating it.

### Step 2b: Replace `print()` calls in `check_pipeline_status`

**BEFORE (`orchestrator/cli.py` lines 159-200):**
```python
def check_pipeline_status(orchestrator: CityBikesOrchestrator):
    """Check and display pipeline status."""
    from db_duckdb.pipeline import check_pipeline_status

    print("=" * 80)
    print("CITY CYCLES PIPELINE STATUS")
    print("=" * 80)

    try:
        status = check_pipeline_status()

        print("\nDatabase Status:")
        print(f"  Tables exist: {'✓' if status['tables_exist'] else '✗'}")
        print(f"  Tables loaded: {'✓' if status['tables_loaded'] else '✗'}")
        print(f"  Marts available: {'✓' if status['marts_available'] else '✗'}")

        if 'details' in status:
            details = status['details']

            if 'total_rows' in details:
                print(f"\nData Volume:")
                print(f"  Total rows: {details['total_rows']:,}")

            if 'existing_tables' in details:
                print(f"\nExisting Tables:")
                for table in details['existing_tables']:
                    print(f"  - {table}")

            if 'missing_tables' in details and details['missing_tables']:
                print(f"\nMissing Tables:")
                for table in details['missing_tables']:
                    print(f"  - {table}")

            if 'mart_tables' in details and details['mart_tables']:
                print(f"\nAvailable Marts:")
                for table in details['mart_tables']:
                    print(f"  - {table}")

        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\nError checking status: {e}")
```

**AFTER:**
```python
def check_pipeline_status(orchestrator: CityBikesOrchestrator):
    """Check and display pipeline status."""
    from db_duckdb.pipeline import check_pipeline_status

    _log_section("CITY CYCLES PIPELINE STATUS")

    try:
        status = check_pipeline_status()

        logger.info("\nDatabase Status:")
        logger.info(f"  Tables exist: {'✓' if status['tables_exist'] else '✗'}")
        logger.info(f"  Tables loaded: {'✓' if status['tables_loaded'] else '✗'}")
        logger.info(f"  Marts available: {'✓' if status['marts_available'] else '✗'}")

        if 'details' in status:
            details = status['details']

            if 'total_rows' in details:
                logger.info("\nData Volume:")
                logger.info(f"  Total rows: {details['total_rows']:,}")

            if 'existing_tables' in details:
                logger.info("\nExisting Tables:")
                for table in details['existing_tables']:
                    logger.info(f"  - {table}")

            if 'missing_tables' in details and details['missing_tables']:
                logger.info("\nMissing Tables:")
                for table in details['missing_tables']:
                    logger.info(f"  - {table}")

            if 'mart_tables' in details and details['mart_tables']:
                logger.info("\nAvailable Marts:")
                for table in details['mart_tables']:
                    logger.info(f"  - {table}")

        logger.info("")

    except Exception as e:
        logger.error(f"Error checking status: {e}")
```

### Lines Affected in `orchestrator/cli.py`

- Line 12: add `_log_section` import
- Add line 14: `logger = logging.getLogger(__name__)`
- Lines 163-200: replace `print()` with `logger.info()` / `logger.error()` and use `_log_section`

---

## Change 3: Narrow Exception Handling in `extraction/nyc.py`

**Problem:** The `download_and_store_zip` function (line 86) catches bare `Exception`, which swallows unexpected errors like `KeyboardInterrupt` (in Python 2 -- not in Python 3, but still a code smell), `PermissionError`, `MemoryError`, etc. The function should catch the specific exceptions that can occur during S3 download and upload operations.

### Step 3a: Add logging and specific imports

**BEFORE (lines 1-8):**
```python
from dotenv import load_dotenv
load_dotenv()
import os
import re
import boto3
from datetime import datetime
import zipfile
from extraction.utils import upload_to_s3, file_exists_in_s3
```

**AFTER (lines 1-11):**
```python
from dotenv import load_dotenv
load_dotenv()
import os
import re
import logging
import boto3
from datetime import datetime
import zipfile
from botocore.exceptions import ClientError
from extraction.utils import upload_to_s3, file_exists_in_s3

logger = logging.getLogger(__name__)
```

### Step 3b: Replace `print()` with `logger` and narrow exceptions in `download_and_store_zip`

**BEFORE (`extraction/nyc.py` lines 57-94):**
```python
def download_and_store_zip(key):
    """
    Download a ZIP file and store it in the raw_zip_files folder in S3.
    Returns True if the file was downloaded and stored, False if it already exists.
    """
    fname = os.path.basename(key)
    local_path = os.path.join(LOCAL_TMP_DIR, fname)
    s3_key = f"{RAW_ZIP_PREFIX}/{fname}"

    # Check if we already have this ZIP file
    if file_exists_in_s3(s3_key):
        print(f"ZIP file already exists in S3: {s3_key}")
        return False

    try:
        # Download the ZIP file
        download_file_from_s3(NYC_PUBLIC_BUCKET, key, local_path)

        # Validate the ZIP file
        if not is_valid_zip(local_path):
            print(f"ERROR: Invalid ZIP file: {local_path}")
            os.remove(local_path)
            return False

        # Upload to our S3 bucket
        upload_to_s3(local_path, s3_key)
        print(f"Stored ZIP file in S3: {s3_key}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to process {key}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
```

**AFTER:**
```python
def download_and_store_zip(key):
    """
    Download a ZIP file and store it in the raw_zip_files folder in S3.
    Returns True if the file was downloaded and stored, False if it already exists.
    """
    fname = os.path.basename(key)
    local_path = os.path.join(LOCAL_TMP_DIR, fname)
    s3_key = f"{RAW_ZIP_PREFIX}/{fname}"

    # Check if we already have this ZIP file
    if file_exists_in_s3(s3_key):
        logger.info(f"ZIP file already exists in S3: {s3_key}")
        return False

    try:
        # Download the ZIP file
        download_file_from_s3(NYC_PUBLIC_BUCKET, key, local_path)

        # Validate the ZIP file
        if not is_valid_zip(local_path):
            logger.error(f"Invalid ZIP file: {local_path}")
            os.remove(local_path)
            return False

        # Upload to our S3 bucket
        upload_to_s3(local_path, s3_key)
        logger.info(f"Stored ZIP file in S3: {s3_key}")
        return True

    except (ClientError, ConnectionError, OSError) as e:
        logger.error(f"Failed to process {key}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
```

**Why these 3 exception types:**
- `ClientError` (from botocore): S3 access denied, bucket not found, throttling, etc.
- `ConnectionError` (built-in): Network connectivity failures during S3 download/upload
- `OSError` (built-in): Disk full when writing temp file, permission errors on `/tmp/`, etc.

Any other exception (e.g., `TypeError`, `ValueError`) indicates a programming bug and should propagate up for visibility rather than being silently caught and returning `False`.

### Step 3c: Replace remaining `print()` calls in other functions

**`list_nyc_citibike_files` (lines 26, 41-42):**

**BEFORE:**
```python
    print(f"Listing files in s3://{NYC_PUBLIC_BUCKET}/ ...")
```

**AFTER:**
```python
    logger.info(f"Listing files in s3://{NYC_PUBLIC_BUCKET}/ ...")
```

**BEFORE:**
```python
    print(f"Matched {len(files)} files for years {start_year}-{end_year}.")
    print(f"Sample files: {files[:5]}")
```

**AFTER:**
```python
    logger.info(f"Matched {len(files)} files for years {start_year}-{end_year}.")
    logger.debug(f"Sample files: {files[:5]}")
```

(Note: sample files list is debug-level because it is only useful for troubleshooting, not normal operation.)

**`download_file_from_s3` (line 46):**

**BEFORE:**
```python
    print(f"Downloading s3://{bucket}/{key} to {dest_path} ...")
```

**AFTER:**
```python
    logger.info(f"Downloading s3://{bucket}/{key} to {dest_path} ...")
```

**`download_all_zips` (lines 100-116):**

**BEFORE:**
```python
    print(f"Using S3 bucket: {S3_BUCKET}")
```

**AFTER:**
```python
    logger.info(f"Using S3 bucket: {S3_BUCKET}")
```

**BEFORE:**
```python
    print(f"Found {len(files)} files to process.")
```

**AFTER:**
```python
    logger.info(f"Found {len(files)} files to process.")
```

**BEFORE (lines 113-116):**
```python
    print(f"\nDownload Summary:")
    print(f"Total files found: {len(files)}")
    print(f"New files downloaded: {downloaded_count}")
    print(f"Files already in S3: {skipped_count}")
```

**AFTER:**
```python
    logger.info("Download Summary:")
    logger.info(f"Total files found: {len(files)}")
    logger.info(f"New files downloaded: {downloaded_count}")
    logger.info(f"Files already in S3: {skipped_count}")
```

### Lines Affected in `extraction/nyc.py`

- Lines 1-8: add `logging`, `ClientError` imports and `logger` definition
- Line 26: `print` -> `logger.info`
- Lines 41-42: `print` -> `logger.info` / `logger.debug`
- Line 46: `print` -> `logger.info`
- Line 68: `print` -> `logger.info`
- Line 77: `print(f"ERROR: ...")` -> `logger.error`
- Line 83: `print` -> `logger.info`
- Lines 86-87: narrow `except Exception` to `except (ClientError, ConnectionError, OSError)`; `print` -> `logger.error`
- Lines 100, 102, 113-116: `print` -> `logger.info`

---

## Change 4: Narrow Exception Handling in `extraction/london.py`

**Problem:** Same as Change 3 -- `download_and_store_csv` (line 96) catches bare `Exception`. Additionally, all output uses `print()`.

### Step 4a: Add logging and specific imports

**BEFORE (lines 1-11):**
```python
from dotenv import load_dotenv
load_dotenv()
import os
import re
import asyncio
from datetime import datetime
from urllib.parse import urljoin
from extraction.utils import upload_to_s3, file_exists_in_s3
from playwright.async_api import async_playwright
import time
import requests
```

**AFTER (lines 1-14):**
```python
from dotenv import load_dotenv
load_dotenv()
import os
import re
import asyncio
import logging
from datetime import datetime
from urllib.parse import urljoin
from extraction.utils import upload_to_s3, file_exists_in_s3
from playwright.async_api import async_playwright
import time
import requests
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)
```

### Step 4b: Replace `print()` and narrow exceptions in `download_and_store_csv`

**BEFORE (`extraction/london.py` lines 57-102):**
```python
def download_and_store_csv(file_url: str, filename: str) -> bool:
    """
    Download a CSV file and store it in the raw CSV folder in S3.
    Returns True if the file was downloaded and stored, False if it already exists.
    """
    # Handle XLS files that are actually CSVs (apparent bug in TfL website)
    if filename.lower().endswith('.xls'):
        s3_filename = os.path.basename(filename)[:-4] + '.csv'
    else:
        s3_filename = os.path.basename(filename)

    s3_key = f"{RAW_CSV_PREFIX}/{s3_filename}"

    # Check if we already have this CSV file
    if file_exists_in_s3(s3_key):
        print(f"CSV file already exists in S3: {s3_key}")
        return False

    try:
        local_path = os.path.join(LOCAL_TMP_DIR, os.path.basename(filename))
        print(f"Downloading {file_url} to {local_path} ...")

        # Download the file
        response = requests.get(file_url)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(response.content)

        # If the file ends with .xls but is actually a CSV, rename it
        if local_path.lower().endswith('.xls'):
            new_local_path = local_path[:-4] + '.csv'
            os.rename(local_path, new_local_path)
            local_path = new_local_path

        # Upload to S3
        upload_to_s3(local_path, s3_key)
        print(f"Stored CSV file in S3: {s3_key}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to process {file_url}: {e}")
        return False
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
```

**AFTER:**
```python
def download_and_store_csv(file_url: str, filename: str) -> bool:
    """
    Download a CSV file and store it in the raw CSV folder in S3.
    Returns True if the file was downloaded and stored, False if it already exists.
    """
    # Handle XLS files that are actually CSVs (apparent bug in TfL website)
    if filename.lower().endswith('.xls'):
        s3_filename = os.path.basename(filename)[:-4] + '.csv'
    else:
        s3_filename = os.path.basename(filename)

    s3_key = f"{RAW_CSV_PREFIX}/{s3_filename}"
    local_path = os.path.join(LOCAL_TMP_DIR, os.path.basename(filename))

    # Check if we already have this CSV file
    if file_exists_in_s3(s3_key):
        logger.info(f"CSV file already exists in S3: {s3_key}")
        return False

    try:
        logger.info(f"Downloading {file_url} to {local_path} ...")

        # Download the file
        response = requests.get(file_url)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(response.content)

        # If the file ends with .xls but is actually a CSV, rename it
        if local_path.lower().endswith('.xls'):
            new_local_path = local_path[:-4] + '.csv'
            os.rename(local_path, new_local_path)
            local_path = new_local_path

        # Upload to S3
        upload_to_s3(local_path, s3_key)
        logger.info(f"Stored CSV file in S3: {s3_key}")
        return True

    except (RequestException, ConnectionError, OSError) as e:
        logger.error(f"Failed to process {file_url}: {e}")
        return False
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
```

**Why these 3 exception types:**
- `RequestException` (from `requests.exceptions`): Covers all `requests` library errors including `HTTPError` (from `raise_for_status()`), `ConnectionError`, `Timeout`, `TooManyRedirects`. This is the base exception for the `requests` library.
- `ConnectionError` (built-in): Network failures during S3 upload via boto3
- `OSError` (built-in): Disk write failures, rename failures, permission errors

**Important bugfix in AFTER:** The `local_path` variable is now assigned BEFORE the `try` block (moved from inside the `try` to line 70). In the original code, if the `try` block raised an exception before `local_path` was assigned, the `finally` block would raise `UnboundLocalError` when it tried to access `local_path`. This is a pre-existing bug that this change fixes.

### Step 4c: Replace remaining `print()` calls

**`download_all_csvs` (lines 108-126):**

**BEFORE:**
```python
    print(f"Using S3 bucket: {os.environ.get('S3_BUCKET')}")
    print("Attempting to download all CSV files from TfL...")
```

**AFTER:**
```python
    logger.info(f"Using S3 bucket: {os.environ.get('S3_BUCKET')}")
    logger.info("Attempting to download all CSV files from TfL...")
```

**BEFORE:**
```python
    print(f"Found {len(files)} files to process.")
```

**AFTER:**
```python
    logger.info(f"Found {len(files)} files to process.")
```

**BEFORE (lines 123-126):**
```python
    print(f"\nDownload Summary:")
    print(f"Total files found: {len(files)}")
    print(f"New files downloaded: {downloaded_count}")
    print(f"Files already in S3: {skipped_count}")
```

**AFTER:**
```python
    logger.info("Download Summary:")
    logger.info(f"Total files found: {len(files)}")
    logger.info(f"New files downloaded: {downloaded_count}")
    logger.info(f"Files already in S3: {skipped_count}")
```

### Lines Affected in `extraction/london.py`

- Lines 1-11: add `logging`, `RequestException` imports and `logger` definition
- Line 72: `print` -> `logger.info`
- Line 77: `print` -> `logger.info`
- Lines 93-94: `print` -> `logger.info`
- Lines 96-97: narrow `except Exception` to `except (RequestException, ConnectionError, OSError)`; `print` -> `logger.error`
- Line 70: move `local_path` assignment before `try` block (bugfix)
- Lines 108-109, 111, 123-126: `print` -> `logger.info`

---

## Change 5: Replace `print()` with `logger` in `extraction/utils.py`

**Problem:** `extraction/utils.py` already imports `logging` (line 6) and uses `logging.error()` at module level (line 10), but `upload_to_s3` (line 23) uses `print()` instead of the logger for consistency.

**BEFORE (`extraction/utils.py` lines 1-24):**
```python
from dotenv import load_dotenv
load_dotenv()

import os
import boto3
import logging

S3_BUCKET = os.environ.get("S3_BUCKET")
if not S3_BUCKET:
    logging.error("S3_BUCKET environment variable is not set! Please set S3_BUCKET before running the script.")
    raise ValueError("S3_BUCKET environment variable is not set!")

private_s3 = boto3.client("s3")

def check_s3_bucket():
    if not S3_BUCKET:
        logging.error("S3_BUCKET environment variable is not set! Please set S3_BUCKET before running the script.")
        raise ValueError("S3_BUCKET environment variable is not set!")
    return S3_BUCKET

def upload_to_s3(local_path, s3_key):
    check_s3_bucket()
    print(f"Uploading CSV: {local_path} to s3://{S3_BUCKET}/{s3_key} ...")
    private_s3.upload_file(local_path, S3_BUCKET, s3_key)
```

**AFTER:**
```python
from dotenv import load_dotenv
load_dotenv()

import os
import boto3
import logging

logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET")
if not S3_BUCKET:
    logger.error("S3_BUCKET environment variable is not set! Please set S3_BUCKET before running the script.")
    raise ValueError("S3_BUCKET environment variable is not set!")

private_s3 = boto3.client("s3")

def check_s3_bucket():
    if not S3_BUCKET:
        logger.error("S3_BUCKET environment variable is not set! Please set S3_BUCKET before running the script.")
        raise ValueError("S3_BUCKET environment variable is not set!")
    return S3_BUCKET

def upload_to_s3(local_path, s3_key):
    check_s3_bucket()
    logger.info(f"Uploading: {local_path} to s3://{S3_BUCKET}/{s3_key} ...")
    private_s3.upload_file(local_path, S3_BUCKET, s3_key)
```

### Lines Affected in `extraction/utils.py`

- Add line 8: `logger = logging.getLogger(__name__)`
- Line 10: `logging.error(...)` -> `logger.error(...)`
- Line 17: `logging.error(...)` -> `logger.error(...)`
- Line 23: `print(...)` -> `logger.info(...)`

---

## What NOT To Do

- **Do NOT change the pipeline stage order or naming.** The stages (`extraction`, `file_management`, `database_load`, `dbt`, `export`) are referenced by the CLI argument parser and by the test suite.
- **Do NOT change the CLI argument names or options.** The `--skip-extraction`, `--skip-verify`, `--skip-export`, `--dbt-full-refresh`, `--verbose`, `--full-refresh` flags and `stage_name` choices must remain identical. Tests in `test_orchestrator.py` verify these.
- **Do NOT modify the dbt subprocess execution logic** in `_run_dbt_transformations` (lines 216-284). The `Popen` + streaming stdout pattern is working correctly and is complex -- changing it risks breaking dbt integration.
- **Do NOT change how `config.py` reads environment variables.** It is out of scope for this PR.
- **Do NOT change the public API of `CityBikesOrchestrator`.** The `run()`, `run_stage()`, and `__init__()` signatures must remain identical.
- **Do NOT change `print()` calls on lines 152 and 155 in `orchestrator/cli.py`.** These are in the `except KeyboardInterrupt` and `except Exception` handlers of `main()` and are appropriate as `print()` because they run during fatal error exit paths.
- **Do NOT change `print()` calls in `orchestrator/config.py`.** The `validate_config()` and `print_config()` functions intentionally use `print()` because they are user-facing display functions that should always output regardless of log level. They are out of scope for this PR.
- **Do NOT modify `extraction/nyc.py` line 21** where `public_s3` is created with unsigned config. This is correct for accessing the public NYC CitiBike S3 bucket.
- **Do NOT remove the `time` import from `extraction/london.py`** -- it is used by `time.time()` in the playwright scrolling logic (even though Phase 01 removes the unused `start_time` variable, the `time` module may still be used by other code or may be needed in the future; check after Phase 01 merges).

---

## Verification Checklist

Run ALL of the following after making changes. Every check must pass before opening the PR.

### 1. Full Test Suite

```bash
python -m pytest tests/ -v
```

Expected: All tests pass. The orchestrator tests (`tests/test_orchestrator.py`) have 31 tests across 4 classes. All must pass.

### 2. Import Smoke Tests

```bash
python -c "from orchestrator.main import CityBikesOrchestrator, _log_section, _log_step; print('main OK')"
python -c "from orchestrator.cli import main, check_pipeline_status; print('cli OK')"
python -c "from extraction.nyc import download_all_zips, list_nyc_citibike_files; print('nyc OK')"
python -c "from extraction.london import download_and_store_csv, process_and_upload_london_files; print('london OK')"
python -c "from extraction.utils import upload_to_s3, file_exists_in_s3; print('utils OK')"
```

All should print their "OK" message with no errors.

### 3. CLI Smoke Tests

```bash
python -m orchestrator.cli --help
python -m orchestrator.cli run --help
python -m orchestrator.cli stage --help
```

All should display help text without errors.

### 4. Orchestrator Tests (Focused)

```bash
python -m pytest tests/test_orchestrator.py -v
```

Pay specific attention to:
- `test_run_full_pipeline_mocked` -- verifies all 5 stages are called
- `test_cli_help_output` -- verifies CLI help works
- `test_orchestrator_results_tracking` -- verifies result dict structure

### 5. Verify No Remaining `print()` in Modified Files (except allowed locations)

```bash
grep -n "print(" orchestrator/main.py
grep -n "print(" extraction/nyc.py
grep -n "print(" extraction/london.py
grep -n "print(" extraction/utils.py
```

Expected results:
- `orchestrator/main.py`: Zero results
- `extraction/nyc.py`: Zero results
- `extraction/london.py`: Zero results
- `extraction/utils.py`: Zero results

```bash
grep -n "print(" orchestrator/cli.py
```

Expected: Only lines 152 and 155 (the KeyboardInterrupt and fatal error handlers in `main()`).

### 6. Verify Exception Types Are Specific

```bash
grep -n "except Exception" extraction/nyc.py
grep -n "except Exception" extraction/london.py
```

Expected: Zero results in both files. No bare `except Exception` should remain.

### 7. Git Diff Review

```bash
git diff --stat
```

Verify exactly these 5 files appear (plus `CHANGELOG.md`):
- `orchestrator/main.py`
- `orchestrator/cli.py`
- `extraction/nyc.py`
- `extraction/london.py`
- `extraction/utils.py`

No other files should be modified.

---

## PR Checklist

- [ ] All 5 changes applied as specified
- [ ] `_log_section` and `_log_step` helpers added to `orchestrator/main.py`
- [ ] All 10 separator patterns in `main.py` replaced with helper calls
- [ ] `check_pipeline_status` in `cli.py` uses `logger` instead of `print()`
- [ ] `extraction/nyc.py` uses `logger` and catches `(ClientError, ConnectionError, OSError)`
- [ ] `extraction/london.py` uses `logger` and catches `(RequestException, ConnectionError, OSError)`
- [ ] `extraction/london.py` `local_path` moved before `try` block (bugfix)
- [ ] `extraction/utils.py` uses `logger` instead of `print()` and `logging.error()`
- [ ] `python -m pytest tests/ -v` passes (all tests green)
- [ ] `python -m pytest tests/test_orchestrator.py -v` passes (31 tests)
- [ ] All import smoke tests pass
- [ ] All CLI smoke tests pass
- [ ] `git diff` shows only the 5 target files (plus `CHANGELOG.md`)
- [ ] No functional behavior changes -- only logging and exception handling improvements
- [ ] CHANGELOG.md updated with entry under `[Unreleased]`

### CHANGELOG Entry

```markdown
### Improved
- **Orchestrator Logging** - Consolidated 26+ repeated separator patterns into `_log_section` and `_log_step` helpers
  - Reduced boilerplate in `orchestrator/main.py` by ~20 lines
  - Pipeline step headers are now generated consistently from a single function

### Fixed
- **London Extraction UnboundLocalError** - Moved `local_path` assignment before `try` block in `extraction/london.py`
  - Previously, if an exception occurred before `local_path` was assigned, the `finally` block would raise `UnboundLocalError`

### Technical Improvements
- **Logging Consistency** - Replaced `print()` with `logging` across orchestrator and extraction modules
  - `orchestrator/cli.py`: `check_pipeline_status` now uses `logger` (18 print calls replaced)
  - `extraction/nyc.py`: All 11 `print()` calls replaced with `logger.info()` / `logger.error()`
  - `extraction/london.py`: All 9 `print()` calls replaced with `logger.info()` / `logger.error()`
  - `extraction/utils.py`: `print()` and `logging.error()` calls replaced with named `logger`
- **Exception Handling** - Narrowed bare `except Exception` to specific exception types
  - `extraction/nyc.py`: Now catches `(ClientError, ConnectionError, OSError)`
  - `extraction/london.py`: Now catches `(RequestException, ConnectionError, OSError)`
```
