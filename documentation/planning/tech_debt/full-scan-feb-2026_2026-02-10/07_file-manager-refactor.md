# Phase 07: Extracted File Manager Refactor

**PR Title:** `refactor(file-manager): fix memory inefficiency in CSV streaming, remove hardcoded schema, cache S3 existence checks`
**Risk Level:** Medium
**Estimated Effort:** Large (4-6 hours)
**Dependencies:** Phase 01 (dead code cleanup must merge first -- it modifies the same file removing unused imports)
**Blocks:** Phase 09 (test coverage expansion)

---

## Summary

Refactor `extracted_file_manager/manager.py` to fix a memory inefficiency where `_stream_csv_to_parquet` loads the entire CSV into memory despite its name suggesting streaming behavior, remove a hardcoded PyArrow schema that only works for NYC Modern data, and eliminate an O(n*m) S3 HEAD request pattern in `_parquet_exists_for_csv`. This PR also extracts duplicated CSV-from-ZIP upload logic into a shared helper.

---

## Files Modified

| # | File | Action |
|---|------|--------|
| 1 | `extracted_file_manager/manager.py` | Major refactor: 4 changes |

**No other files are modified in this PR.** All changes are internal to `manager.py` and do not alter any public API signatures or return types.

---

## Change 1: Fix `_stream_csv_to_parquet` Memory Inefficiency

**Problem:** The method `_stream_csv_to_parquet` (lines 432-516) claims to stream CSV data but actually loads the entire CSV file into memory as a string on line 446, then manually splits it into lines (line 447) and re-joins chunks with header prepended (line 477). For GB+ files, the `response['Body'].read().decode('utf-8')` call causes a memory spike equal to the full file size, defeating the purpose of chunked processing.

**Root Cause:** The S3 `get_object` response body is read all at once. The subsequent line-splitting and chunk-reassembly is an attempt at chunking that still requires the full file to be in memory.

**Fix:** Download the S3 object to a temporary file first, then use `pd.read_csv()` with `chunksize` parameter to stream from disk. This keeps memory usage proportional to `chunk_size` rows, not the total file size.

### BEFORE (`extracted_file_manager/manager.py` lines 432-516):

```python
    @retry_on_transient_error(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def _stream_csv_to_parquet(self, csv_s3_key: str, parquet_s3_key: str, model):
        """Stream CSV to parquet using pyarrow with proper string handling"""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
            temp_parquet_path = temp_parquet.name

        try:
            # Stream CSV from S3 to parquet
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=csv_s3_key)

            # Use pandas with string_dtype to prevent type inference issues
            chunk_size = 10000  # Process in chunks to manage memory

            # Read CSV in chunks using pandas
            csv_content = response['Body'].read().decode('utf-8')
            lines = csv_content.split('\n')
            header = lines[0]

            # Create a StringIO object for pandas
            from io import StringIO

            # Define explicit schema upfront to avoid NULL type inference issues
            parquet_schema = pa.schema([
                ('ride_id', pa.string()),
                ('rideable_type', pa.string()),
                ('started_at', pa.string()),
                ('ended_at', pa.string()),
                ('start_station_id', pa.string()),
                ('start_station_name', pa.string()),
                ('end_station_id', pa.string()),
                ('end_station_name', pa.string()),
                ('start_lat', pa.float64()),
                ('start_lng', pa.float64()),
                ('end_lat', pa.float64()),
                ('end_lng', pa.float64()),
                ('member_casual', pa.string()),
                ('source_file', pa.string())
            ])

            writer = pq.ParquetWriter(temp_parquet_path, parquet_schema)
            chunk_start = 1  # Skip header

            while chunk_start < len(lines):
                chunk_end = min(chunk_start + chunk_size, len(lines))
                chunk_lines = [header] + lines[chunk_start:chunk_end]
                chunk_csv = '\n'.join(chunk_lines)

                # Read chunk with pandas, forcing string types for station IDs and float for lat/lng
                df_chunk = pd.read_csv(StringIO(chunk_csv), dtype={
                    'start station id': str,
                    'end station id': str,
                    'start_station_id': str,
                    'end_station_id': str,
                    'start_station_name': str,
                    'end_station_name': str,
                    'ride_id': str,
                    'rideable_type': str,
                    'member_casual': str,
                    'usertype': str,
                    'bikeid': str,
                    'start_lat': 'float64',
                    'start_lng': 'float64',
                    'end_lat': 'float64',
                    'end_lng': 'float64'
                })

                # Apply model transformation
                df_transformed = model.to_dataframe(df_chunk, csv_s3_key)

                # Convert to pyarrow with explicit schema casting
                table = pa.Table.from_pandas(df_transformed, schema=parquet_schema)

                writer.write_table(table)

                chunk_start = chunk_end

            if writer:
                writer.close()

            # Upload parquet to S3
            with open(temp_parquet_path, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, parquet_s3_key)

        finally:
            os.unlink(temp_parquet_path)
```

### AFTER (`extracted_file_manager/manager.py`):

```python
    @retry_on_transient_error(max_attempts=3, initial_delay=2.0, backoff_factor=2.0)
    def _stream_csv_to_parquet(self, csv_s3_key: str, parquet_s3_key: str, model):
        """Stream CSV to parquet using chunked reading from a temp file.

        Downloads the CSV from S3 to a local temp file, then reads it in chunks
        using pandas to keep memory usage bounded regardless of file size.
        """
        temp_csv_path = None
        temp_parquet_path = None

        try:
            # Create temp files for CSV download and Parquet output
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_csv:
                temp_csv_path = temp_csv.name
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
                temp_parquet_path = temp_parquet.name

            # Download CSV from S3 to temp file (avoids loading into memory)
            self.s3_client.download_file(self.s3_bucket, csv_s3_key, temp_csv_path)

            # Build dtype hints that cover all known column names across all schemas.
            # Columns not present in a given CSV are silently ignored by pandas.
            dtype_hints = {
                'start station id': str,
                'end station id': str,
                'start_station_id': str,
                'end_station_id': str,
                'start_station_name': str,
                'end_station_name': str,
                'ride_id': str,
                'rideable_type': str,
                'member_casual': str,
                'usertype': str,
                'bikeid': str,
                'Rental Id': str,
                'Bike Id': str,
                'StartStation Id': str,
                'EndStation Id': str,
                'Number': str,
                'Bike number': str,
                'Start station number': str,
                'End station number': str,
            }

            chunk_size = 50_000  # Rows per chunk (increased from 10k for better throughput)
            writer = None

            for df_chunk in pd.read_csv(temp_csv_path, chunksize=chunk_size, dtype=dtype_hints):
                # Apply model transformation (renames columns, casts types, adds source_file)
                df_transformed = model.to_dataframe(df_chunk, csv_s3_key)

                # Convert to PyArrow table -- schema is inferred from the DataFrame
                # which has already been standardized by model.to_dataframe()
                table = pa.Table.from_pandas(df_transformed, preserve_index=False)

                # Initialize writer on first chunk using the inferred schema
                if writer is None:
                    writer = pq.ParquetWriter(temp_parquet_path, table.schema)

                writer.write_table(table)

            if writer:
                writer.close()

            # Upload parquet to S3
            with open(temp_parquet_path, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, parquet_s3_key)

        finally:
            # Clean up both temp files
            if temp_csv_path and os.path.exists(temp_csv_path):
                os.unlink(temp_csv_path)
            if temp_parquet_path and os.path.exists(temp_parquet_path):
                os.unlink(temp_parquet_path)
```

### Why This Works

1. **Memory bounded:** `pd.read_csv(..., chunksize=50_000)` reads only 50k rows at a time from disk. The S3 download goes to a temp file, not into Python memory.
2. **Schema-agnostic:** Instead of hardcoding the 14-column NYC Modern schema, the PyArrow schema is inferred from the first chunk's DataFrame. The `model.to_dataframe()` call already standardizes columns and types per the data model class, so the resulting schema is correct for any model (NYC Legacy, NYC Modern, London Legacy, London Modern).
3. **dtype_hints expanded:** The `dtype_hints` dict now includes column names from all four schemas so that string columns are never accidentally parsed as numeric, regardless of which CSV is being processed. Pandas ignores dtype hints for columns not present in the file.
4. **chunk_size increased:** Changed from 10,000 to 50,000 rows. The old code's chunking was artificial (splitting strings) and not actual streaming. With true file-based chunking, 50k rows is a good balance -- about 5-15 MB per chunk depending on row width.
5. **writer initialized lazily:** The `ParquetWriter` is created from the first chunk's inferred schema, so there is no need to hardcode any schema definition.
6. **Removed `from io import StringIO` import** that was inside the function body (line 451). It is no longer needed.

### Lines Affected

- Delete lines 432-516 (the entire old `_stream_csv_to_parquet` method)
- Replace with the new method above in the same location

### Key Risk: Schema Consistency Across Chunks

**Concern:** If PyArrow infers slightly different schemas between chunks (e.g., a column is all-null in one chunk but has values in another), the `ParquetWriter` will reject the second chunk.

**Mitigation:** The `model.to_dataframe()` method on each data model class already:
- Renames columns to canonical names
- Casts station IDs to `str` (NYCModernBikeShareRecord lines 132-134)
- Casts coordinates to `float64` via `pd.to_numeric()` (NYCModernBikeShareRecord lines 137-140)
- Returns only the columns defined in the dataclass (via `cls.__dataclass_fields__.keys()`)

So each chunk produces an identical set of columns with consistent types. This is safe.

---

## Change 2: Extract Duplicated CSV-from-ZIP Upload Logic

**Problem:** The logic for extracting a CSV from a ZIP and uploading it to S3 is duplicated between `_extract_zip_using_filetree` (lines 287-315) and `_process_nested_zip` (lines 331-358). Both contain identical:
- MacOSX artifact skip checks
- `os.path.basename()` for filename extraction
- S3 key construction with hardcoded `"extracted_bike_ride_csvs/nyc/"` prefix
- `_file_exists_in_s3()` check
- `upload_fileobj()` call
- Print statements with same formatting

**Fix:** Extract the shared logic into a private helper method `_upload_csv_from_zip_entry`.

### BEFORE -- duplicated block in `_extract_zip_using_filetree` (lines 287-315):

```python
                    # Handle CSV files directly
                    elif filename.lower().endswith('.csv'):
                        print(f"DEBUG: Found CSV file: {filename}")

                        # Skip MacOSX artifacts
                        if filename.startswith('._') or '__MACOSX' in filename:
                            print(f"  Skipping MacOSX artifact: {filename}")
                            continue

                        # Get just the filename (not the path)
                        csv_filename = os.path.basename(filename)
                        csv_s3_key = f"extracted_bike_ride_csvs/nyc/{csv_filename}"

                        # Check if CSV already exists
                        if self._file_exists_in_s3(csv_s3_key):
                            print(f"  Skipping {csv_filename} - already exists")
                            skipped_count += 1
                            continue

                        # Stream CSV content directly to S3 (memory-efficient)
                        # Open the file from ZIP as a stream
                        with zf.open(filename) as csv_stream:
                            self.s3_client.upload_fileobj(
                                csv_stream,
                                self.s3_bucket,
                                csv_s3_key
                            )
                        print(f"  ✓ Uploaded {csv_filename}")
                        csv_count += 1
```

### BEFORE -- same block duplicated in `_process_nested_zip` (lines 331-358):

```python
                for filename in nested_zf.namelist():
                    if filename.lower().endswith('.csv'):
                        print(f"DEBUG: Found CSV file: {filename}")

                        # Skip MacOSX artifacts
                        if filename.startswith('._') or '__MACOSX' in filename:
                            print(f"  Skipping MacOSX artifact: {filename}")
                            continue

                        # Get just the filename (not the path)
                        csv_filename = os.path.basename(filename)
                        csv_s3_key = f"extracted_bike_ride_csvs/nyc/{csv_filename}"

                        # Check if CSV already exists
                        if self._file_exists_in_s3(csv_s3_key):
                            print(f"  Skipping {csv_filename} - already exists")
                            skipped_count += 1
                            continue

                        # Stream CSV content directly to S3 (memory-efficient)
                        with nested_zf.open(filename) as csv_stream:
                            self.s3_client.upload_fileobj(
                                csv_stream,
                                self.s3_bucket,
                                csv_s3_key
                            )
                        print(f"  ✓ Uploaded {csv_filename}")
                        csv_count += 1
```

### AFTER -- new helper method (add between `_process_nested_zip` and `_convert_csv_to_parquet`):

```python
    def _upload_csv_from_zip_entry(self, zf: zipfile.ZipFile, filename: str) -> str:
        """Upload a single CSV entry from a ZIP file to S3.

        Args:
            zf: An open ZipFile object containing the CSV
            filename: The filename/path within the ZIP archive

        Returns:
            'uploaded' if the file was uploaded to S3
            'skipped' if the file already exists in S3
            'artifact' if the file is a MacOSX artifact (not uploaded)
        """
        # Skip MacOSX artifacts
        if filename.startswith('._') or '__MACOSX' in filename:
            print(f"  Skipping MacOSX artifact: {filename}")
            return 'artifact'

        csv_filename = os.path.basename(filename)
        csv_s3_key = f"extracted_bike_ride_csvs/nyc/{csv_filename}"

        # Idempotency: check if CSV already exists
        if self._file_exists_in_s3(csv_s3_key):
            print(f"  Skipping {csv_filename} - already exists")
            return 'skipped'

        # Stream CSV content directly to S3
        with zf.open(filename) as csv_stream:
            self.s3_client.upload_fileobj(
                csv_stream,
                self.s3_bucket,
                csv_s3_key
            )
        print(f"  ✓ Uploaded {csv_filename}")
        return 'uploaded'
```

### AFTER -- updated `_extract_zip_using_filetree` (lines 287-315 replaced):

```python
                    # Handle CSV files directly
                    elif filename.lower().endswith('.csv'):
                        print(f"DEBUG: Found CSV file: {filename}")
                        result = self._upload_csv_from_zip_entry(zf, filename)
                        if result == 'uploaded':
                            csv_count += 1
                        elif result == 'skipped':
                            skipped_count += 1
```

### AFTER -- updated `_process_nested_zip` inner loop (lines 331-358 replaced):

```python
            with zipfile.ZipFile(nested_zip_path, 'r') as nested_zf:
                for filename in nested_zf.namelist():
                    if filename.lower().endswith('.csv'):
                        print(f"DEBUG: Found CSV file: {filename}")
                        result = self._upload_csv_from_zip_entry(nested_zf, filename)
                        if result == 'uploaded':
                            csv_count += 1
                        elif result == 'skipped':
                            skipped_count += 1
```

### Lines Affected

- Lines 287-315 in `_extract_zip_using_filetree`: replace 28 lines with 6 lines
- Lines 331-358 in `_process_nested_zip`: replace 27 lines with 6 lines
- Add new method `_upload_csv_from_zip_entry` (~25 lines) between `_process_nested_zip` and `_convert_csv_to_parquet` (between current lines 365 and 367)

### Note on MacOSX Artifact Filtering

The artifact check (`filename.startswith('._') or '__MACOSX' in filename`) was already present on line 261 in the outer loop of `_extract_zip_using_filetree`, AND again on lines 291-293 inside the CSV handling branch. The outer check catches files before they reach the CSV/ZIP branching logic, but only checks for `filename.endswith('/')`, `filename.startswith('._')`, and `'__MACOSX' in filename`. The inner check in the CSV branch is redundant with the outer check. However, the new `_upload_csv_from_zip_entry` keeps the artifact check as a safety measure since the helper is also called from `_process_nested_zip` where the outer check does not exist. This is intentional.

---

## Change 3: Cache S3 Parquet Listings in `_parquet_exists_for_csv`

**Problem:** The method `_parquet_exists_for_csv` (lines 520-536) checks whether a Parquet file already exists for a given CSV. It does this by calling `_file_exists_in_s3()` (which makes an S3 `HEAD` request) for every combination of city (2) and schema (4), totaling up to 8 S3 HEAD requests per CSV file. When processing 200+ CSVs, this is 1,600+ HEAD requests just for existence checking.

### BEFORE (`extracted_file_manager/manager.py` lines 520-536):

```python
    def _parquet_exists_for_csv(self, csv_file: str) -> bool:
        """Check if parquet already exists for this CSV"""
        csv_filename = csv_file.split('/')[-1]
        parquet_filename = csv_filename.replace('.csv', '.parquet')

        # Check all possible schema locations
        cities = ["nyc", "london"]
        schemas = ["nyclegacybikesharerecord", "nycmodernbikesharerecord",
                  "londonlegacybikesharerecord", "londonmodernbikesharerecord"]

        for city in cities:
            for schema in schemas:
                parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
                if self._file_exists_in_s3(parquet_s3_key):
                    return True

        return False
```

### AFTER -- two changes needed:

**Step 3a: Add a cache-building method and an instance variable.**

Add this line to `__init__` (after line 114, after `self.batch_size = batch_size`):

```python
        self._parquet_key_cache: Optional[set] = None  # Lazy-loaded cache of existing parquet S3 keys
```

Add this new method immediately before `_parquet_exists_for_csv`:

```python
    def _build_parquet_cache(self) -> set:
        """Build a set of all existing parquet S3 keys for fast existence checks.

        Uses a single S3 list_objects_v2 call (with pagination) instead of
        individual HEAD requests per file. The cache is stored as a set of
        S3 keys for O(1) lookup.
        """
        parquet_keys = set()
        prefix = "extracted_bike_ride_parquet/"

        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                parquet_keys.add(obj['Key'])

        print(f"  Cached {len(parquet_keys)} existing parquet files")
        return parquet_keys
```

**Step 3b: Replace `_parquet_exists_for_csv` to use the cache.**

```python
    def _parquet_exists_for_csv(self, csv_file: str) -> bool:
        """Check if parquet already exists for this CSV using a cached S3 listing.

        On the first call, builds a cache of all parquet S3 keys via a single
        paginated list_objects_v2 call. Subsequent calls use O(1) set lookups.
        """
        # Lazy-initialize the cache on first call
        if self._parquet_key_cache is None:
            self._parquet_key_cache = self._build_parquet_cache()

        csv_filename = csv_file.split('/')[-1]
        parquet_filename = csv_filename.replace('.csv', '.parquet')

        # Check all possible schema locations against the cache
        cities = ["nyc", "london"]
        schemas = ["nyclegacybikesharerecord", "nycmodernbikesharerecord",
                  "londonlegacybikesharerecord", "londonmodernbikesharerecord"]

        for city in cities:
            for schema in schemas:
                parquet_s3_key = f"extracted_bike_ride_parquet/{city}/{schema}/{parquet_filename}"
                if parquet_s3_key in self._parquet_key_cache:
                    return True

        return False
```

### Lines Affected

- Line 114: add instance variable after `self.batch_size = batch_size`
- Lines 520-536: replace `_parquet_exists_for_csv` with new version
- Add `_build_parquet_cache` method immediately before `_parquet_exists_for_csv`

### Performance Impact

- **Before:** 8 HEAD requests per CSV * ~200 CSVs = ~1,600 S3 API calls
- **After:** 1 paginated LIST call (even with 1,000+ parquet files, this is 1-2 API calls) + O(1) set lookups
- **Net savings:** ~1,598 S3 API calls, plus significant wall-clock time savings (each HEAD request has ~50-100ms latency)

### Cache Invalidation Note

The cache is built once per `ExtractedFileManager` instance lifetime. Since `convert_all_csvs_simple()` is the only caller of `_parquet_exists_for_csv`, and it runs before any new Parquet files are created (the cache is only used to decide what to SKIP), there is no stale-cache risk. Newly converted Parquet files in the current run are not checked against the cache -- they are checked via `_file_exists_in_s3()` inside `_convert_csv_to_parquet` (line 402) which is a separate, uncached check that runs AFTER schema detection.

---

## Change 4: Add Type Import for `Optional` (if not already present post-Phase 01)

**What:** After Phase 01 removes unused imports, verify that `Optional` is still imported from `typing` on line 21. It is needed for the new `self._parquet_key_cache: Optional[set] = None` type annotation added in Change 3.

### Current import (line 21):

```python
from typing import List, Dict, Optional
```

**Action:** No change needed -- `Optional` is already imported. Just verify this line still exists after Phase 01 merges.

---

## Complete Method Ordering After All Changes

After all 4 changes, the methods in `ExtractedFileManager` should appear in this order (same as current, with one new method inserted):

1. `__init__` (updated: new instance variable)
2. `_log_memory_usage` (unchanged)
3. `_cleanup_memory` (unchanged)
4. `extract_all_zips_simple` (unchanged)
5. `convert_all_csvs_simple` (unchanged)
6. `run_simplified_pipeline` (unchanged)
7. `_extract_zip_using_filetree` (updated: uses `_upload_csv_from_zip_entry`)
8. `_process_nested_zip` (updated: uses `_upload_csv_from_zip_entry`)
9. **`_upload_csv_from_zip_entry`** (NEW)
10. `_convert_csv_to_parquet` (unchanged)
11. `_find_matching_model` (unchanged)
12. `_stream_csv_to_parquet` (REWRITTEN -- Change 1)
13. **`_build_parquet_cache`** (NEW)
14. `_parquet_exists_for_csv` (REWRITTEN -- Change 3)
15. `_file_exists_in_s3` (unchanged)
16. `_list_s3_files` (unchanged)

---

## What NOT To Do

- **Do NOT change the S3 path structure.** The paths `extracted_bike_ride_csvs/nyc/`, `extracted_bike_ride_parquet/{city}/{schema}/` etc. are used by `db_duckdb/operations.py` for DuckDB raw table loading. Changing them would break the database load stage.
- **Do NOT change the Parquet file naming convention.** Files are named `{original_csv_name}.parquet` and this convention is assumed by downstream consumers.
- **Do NOT modify the data model classes** in `data_models/nyc_bike.py` or `data_models/london_bike.py`. The `to_dataframe()` and `validate_schema()` methods are the contract between models and the file manager.
- **Do NOT change the public API of `ExtractedFileManager`.** The three public methods (`run_simplified_pipeline`, `extract_all_zips_simple`, `convert_all_csvs_simple`) must keep their current signatures and return types. They are called by `simplified_pipeline.py` (lines 71-74) and by tests.
- **Do NOT change how the `retry_on_transient_error` decorator works.** It wraps `_convert_csv_to_parquet` and `_stream_csv_to_parquet` and must continue to do so.
- **Do NOT remove the `self.s3_client.download_file()` call in `_extract_zip_using_filetree` (line 245).** This downloads the ZIP to a temp file for extraction and is correct behavior.
- **Do NOT change `_convert_csv_to_parquet`** (lines 367-412). It already works correctly -- the schema detection (1MB sample download), model matching, and call to `_stream_csv_to_parquet` are all fine. Only `_stream_csv_to_parquet` itself needs to change.
- **Do NOT remove the `from io import StringIO` line** if it exists at the module level. After this change, it is no longer needed inside `_stream_csv_to_parquet`, but if Phase 01 has not yet removed it, leave module-level imports alone. (Note: in the current code, `StringIO` is imported inside the function body on line 451, not at module level, so nothing needs to be removed from the module-level imports.)

---

## Verification Checklist

Run ALL of the following after making changes. Every check must pass before opening the PR.

### 1. Full Test Suite

```bash
python -m pytest tests/ -v
```

Expected: All tests pass. The file manager tests (`tests/test_extracted_file_manager_current.py`) have 15 tests in `TestExtractedFileManager` and 4 tests in `TestDataModelsIntegration`. All 19 must pass.

### 2. Import Smoke Test

```bash
python -c "from extracted_file_manager.manager import ExtractedFileManager; print('manager import OK')"
python -c "from extracted_file_manager import ExtractedFileManager, run_full_pipeline; print('package import OK')"
```

Both should print their "OK" message with no errors.

### 3. File Manager Unit Tests (Focused)

```bash
python -m pytest tests/test_extracted_file_manager_current.py -v
```

Pay specific attention to:
- `test_parquet_exists_for_csv` -- This test mocks `head_object` to return `{}` (file exists). After your change, this test may need updating because `_parquet_exists_for_csv` now uses the cache instead of `head_object`. See "Test Updates Required" section below.
- `test_parquet_exists_for_csv_not_found` -- Same concern; currently mocks `head_object` to raise `ClientError`.

### 4. Verify No Hardcoded NYC Modern Schema Remains

```bash
grep -n "pa.schema" extracted_file_manager/manager.py
```

Expected: Zero results. The hardcoded `parquet_schema = pa.schema([...])` has been removed.

### 5. Verify No Full-File Read Remains

```bash
grep -n "response\['Body'\].read()" extracted_file_manager/manager.py
```

Expected: This should only appear in `_convert_csv_to_parquet` (line 383, the 1MB sample read for schema detection). It should NOT appear in `_stream_csv_to_parquet`.

### 6. Verify StringIO Removed from `_stream_csv_to_parquet`

```bash
grep -n "StringIO" extracted_file_manager/manager.py
```

Expected: Zero results (the `from io import StringIO` was only inside the old function body).

### 7. Git Diff Review

```bash
git diff --stat
```

Verify only `extracted_file_manager/manager.py` appears (plus `CHANGELOG.md`). No other files should be modified.

---

## Test Updates Required

The existing tests in `tests/test_extracted_file_manager_current.py` mock S3 at the `head_object` level. After Change 3, `_parquet_exists_for_csv` no longer calls `head_object` -- it uses the cache built from `list_objects_v2`. Two tests need updating:

### Update `test_parquet_exists_for_csv` (lines 195-201)

**BEFORE:**
```python
    def test_parquet_exists_for_csv(self, manager, mock_s3_client):
        """Test checking if parquet exists for CSV"""
        # Mock that a parquet file exists
        mock_s3_client.head_object.return_value = {}

        result = manager._parquet_exists_for_csv("extracted_bike_ride_csvs/nyc/test.csv")
        assert result is True
```

**AFTER:**
```python
    def test_parquet_exists_for_csv(self, manager, mock_s3_client):
        """Test checking if parquet exists for CSV (cache-based)"""
        # Mock the paginator for list_objects_v2
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'extracted_bike_ride_parquet/nyc/nycmodernbikesharerecord/test.parquet'}
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator

        result = manager._parquet_exists_for_csv("extracted_bike_ride_csvs/nyc/test.csv")
        assert result is True
        mock_s3_client.get_paginator.assert_called_once_with('list_objects_v2')
```

### Update `test_parquet_exists_for_csv_not_found` (lines 203-212)

**BEFORE:**
```python
    def test_parquet_exists_for_csv_not_found(self, manager, mock_s3_client):
        """Test checking if parquet exists for CSV (not found)"""
        # Mock that no parquet file exists
        mock_s3_client.head_object.side_effect = ClientError(
            error_response={'Error': {'Code': '404', 'Message': 'Not Found'}},
            operation_name='HeadObject'
        )

        result = manager._parquet_exists_for_csv("extracted_bike_ride_csvs/nyc/test.csv")
        assert result is False
```

**AFTER:**
```python
    def test_parquet_exists_for_csv_not_found(self, manager, mock_s3_client):
        """Test checking if parquet exists for CSV (not found, cache-based)"""
        # Mock an empty paginator result (no parquet files exist)
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'Contents': []}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator

        result = manager._parquet_exists_for_csv("extracted_bike_ride_csvs/nyc/test.csv")
        assert result is False
```

### Important: Reset Cache Between Tests

Each test creates a fresh `manager` fixture, so `self._parquet_key_cache` starts as `None` for each test. No cache reset is needed between tests.

---

## PR Checklist

- [ ] All 4 changes applied as specified
- [ ] Two existing tests updated for cache-based `_parquet_exists_for_csv`
- [ ] `python -m pytest tests/ -v` passes (all tests green)
- [ ] `python -m pytest tests/test_extracted_file_manager_current.py -v` passes (19 tests)
- [ ] Import smoke tests pass
- [ ] No hardcoded `pa.schema([...])` remains in manager.py
- [ ] No full-file `response['Body'].read()` in `_stream_csv_to_parquet`
- [ ] `git diff` shows only `extracted_file_manager/manager.py` and `tests/test_extracted_file_manager_current.py` (plus `CHANGELOG.md`)
- [ ] No public API signatures changed
- [ ] CHANGELOG.md updated with entry under `[Unreleased]`

### CHANGELOG Entry

```markdown
### Improved
- **File Manager Memory Efficiency** - Fixed `_stream_csv_to_parquet` loading entire CSV into memory
  - Now downloads CSV to temp file and uses `pd.read_csv(chunksize=50_000)` for true streaming
  - Memory usage is now proportional to chunk size, not total file size
  - Removed hardcoded PyArrow schema that only worked for NYC Modern data; schema is now inferred from model transformation output
- **S3 Existence Check Performance** - Replaced O(n*m) HEAD requests with cached S3 listing
  - `_parquet_exists_for_csv` now uses a single paginated `list_objects_v2` call instead of up to 8 HEAD requests per CSV
  - Reduces ~1,600 S3 API calls to ~2 for a typical pipeline run

### Technical Improvements
- **Extracted Duplicated ZIP Upload Logic** - Consolidated identical CSV-from-ZIP upload code into `_upload_csv_from_zip_entry` helper
  - Removed ~50 lines of duplicated code between `_extract_zip_using_filetree` and `_process_nested_zip`
```
