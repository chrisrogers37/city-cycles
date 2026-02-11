# Phase 04: Data Models Base Class Consolidation

**PR Title:** `refactor: consolidate schema validation into base class, replace debug prints with logging`
**Risk Level:** Low
**Estimated Effort:** Small (1-2 hours)
**Dependencies:** Phase 01 (dead code cleanup must merge first -- touches same files)
**Blocks:** Phase 09 (test coverage)

---

## Summary

All 4 data model subclasses (`NYCLegacyBikeShareRecord`, `NYCModernBikeShareRecord`, `LondonLegacyBikeShareRecord`, `LondonModernBikeShareRecord`) contain **identical** `validate_schema` classmethod implementations. The only difference between the four copies is the class name string used in the debug print statement. The base class `BaseBikeShareRecord` currently declares `validate_schema` as raising `NotImplementedError`, forcing each subclass to reimplement it.

This PR:
1. Moves the shared `validate_schema` logic into the base class so it uses `cls._required_columns` and `cls.__name__` polymorphically.
2. Replaces the environment-variable-gated `print()` debug output with Python's standard `logging` module at `DEBUG` level.
3. Deletes the four duplicate `validate_schema` methods from the subclass files.

---

## Files Modified

| # | File | Action |
|---|------|--------|
| 1 | `data_models/base.py` | Implement `validate_schema` in the base class; add `logging` import; declare `_required_columns` class attribute |
| 2 | `data_models/nyc_bike.py` | Delete `validate_schema` from `NYCLegacyBikeShareRecord` (lines 50-59) and `NYCModernBikeShareRecord` (lines 115-124); remove unused `os` import if no longer needed |
| 3 | `data_models/london_bike.py` | Delete `validate_schema` from `LondonLegacyBikeShareRecord` (lines 39-48) and `LondonModernBikeShareRecord` (lines 100-109); remove unused `os` import if no longer needed |

---

## The Problem in Detail

### Current base class (`data_models/base.py`, lines 18-25)

```python
@classmethod
def validate_schema(cls, df: pd.DataFrame) -> bool:
    """Validate if the dataframe contains all required columns.

    This method should be implemented by subclasses to check if the dataframe
    contains the expected columns. Type validation is handled during transformation.
    """
    raise NotImplementedError("Subclasses must implement validate_schema")
```

### Current duplicated pattern (repeated 4 times, shown here for `NYCLegacyBikeShareRecord`)

**File:** `data_models/nyc_bike.py`, lines 50-59

```python
@classmethod
def validate_schema(cls, df: pd.DataFrame) -> bool:
    """Validate if the dataframe contains all required columns for legacy NYC format."""
    debug_mode = os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG') == '1'

    missing_columns = [col for col in cls._required_columns if col not in df.columns]
    if missing_columns and debug_mode:
        print(f"DEBUG: NYCLegacyBikeShareRecord validation failed - missing columns: {missing_columns}")
        print(f"DEBUG: Available columns: {list(df.columns)}")
    return not missing_columns
```

The other three copies are at:
- `data_models/nyc_bike.py`, lines 115-124 (`NYCModernBikeShareRecord`) -- identical logic, only the class name string differs (`"NYCModernBikeShareRecord"`)
- `data_models/london_bike.py`, lines 39-48 (`LondonLegacyBikeShareRecord`) -- identical logic, only the class name string differs (`"LondonLegacyBikeShareRecord"`)
- `data_models/london_bike.py`, lines 100-109 (`LondonModernBikeShareRecord`) -- identical logic, only the class name string differs (`"LondonModernBikeShareRecord"`)

### Why the debug `print()` is problematic

1. **Not controllable at runtime.** It requires a specific environment variable (`EXTRACTED_FILE_MANAGER_DEBUG=1`) instead of using Python's built-in logging levels.
2. **Writes to stdout.** In a production pipeline, `print()` output mixes with regular program output and is difficult to filter.
3. **Only fires when both conditions are true.** If `debug_mode` is off, you get zero diagnostic output even when validation fails, making production debugging harder.

---

## Detailed Changes

### Change 1: `data_models/base.py` -- Implement `validate_schema` in the base class

**BEFORE** (full file, 37 lines):

```python
import os
import sys
import pandas as pd
from typing import Type, List
from datetime import datetime
import numpy as np

class BaseBikeShareRecord:
    staging_table: str = None
    s3_prefix: str = None
    _registry: List[Type['BaseBikeShareRecord']] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls not in BaseBikeShareRecord._registry:
            BaseBikeShareRecord._registry.append(cls)

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns.

        This method should be implemented by subclasses to check if the dataframe
        contains the expected columns. Type validation is handled during transformation.
        """
        raise NotImplementedError("Subclasses must implement validate_schema")

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Transform raw dataframe into standardized model format.

        This method should be implemented by subclasses to:
        1. Rename columns to match model's field names
        2. Convert data types as needed
        3. Add source_file column
        4. Return only the columns defined in the model
        """
        raise NotImplementedError("Subclasses must implement to_dataframe")
```

**AFTER** (full file):

```python
import os
import sys
import logging
import pandas as pd
from typing import Type, List
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class BaseBikeShareRecord:
    staging_table: str = None
    s3_prefix: str = None
    _required_columns: list = []
    _registry: List[Type['BaseBikeShareRecord']] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls not in BaseBikeShareRecord._registry:
            BaseBikeShareRecord._registry.append(cls)

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns.

        Uses cls._required_columns defined in each subclass to check whether
        the given DataFrame has every expected column. Logs missing columns
        at DEBUG level so diagnostics are available without custom env vars.
        """
        missing_columns = [col for col in cls._required_columns if col not in df.columns]
        if missing_columns:
            logger.debug(
                "%s validation failed - missing columns: %s. Available: %s",
                cls.__name__,
                missing_columns,
                list(df.columns),
            )
        return not missing_columns

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Transform raw dataframe into standardized model format.

        This method should be implemented by subclasses to:
        1. Rename columns to match model's field names
        2. Convert data types as needed
        3. Add source_file column
        4. Return only the columns defined in the model
        """
        raise NotImplementedError("Subclasses must implement to_dataframe")
```

**What changed line-by-line:**

| Line(s) | Change |
|----------|--------|
| 3 | Added `import logging` |
| 10 | Added `logger = logging.getLogger(__name__)` |
| 15 | Added `_required_columns: list = []` class attribute (provides safe default for the base class) |
| 23-35 | Replaced `raise NotImplementedError(...)` body with the shared validation logic using `cls._required_columns` and `cls.__name__`; replaced `print()` with `logger.debug()`; removed env-var gate |

### Change 2: `data_models/nyc_bike.py` -- Remove duplicate `validate_schema` methods

**Step 2a: Remove `NYCLegacyBikeShareRecord.validate_schema` (lines 50-59)**

Delete these 10 lines entirely:

```python
    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns for legacy NYC format."""
        debug_mode = os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG') == '1'

        missing_columns = [col for col in cls._required_columns if col not in df.columns]
        if missing_columns and debug_mode:
            print(f"DEBUG: NYCLegacyBikeShareRecord validation failed - missing columns: {missing_columns}")
            print(f"DEBUG: Available columns: {list(df.columns)}")
        return not missing_columns
```

After deletion, the class goes directly from the `_required_columns` list (ending at line 48) to the `to_dataframe` method (previously at line 61). Add a blank line between them for readability.

**Step 2b: Remove `NYCModernBikeShareRecord.validate_schema` (lines 115-124)**

Delete these 10 lines entirely:

```python
    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns for modern NYC format."""
        debug_mode = os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG') == '1'

        missing_columns = [col for col in cls._required_columns if col not in df.columns]
        if missing_columns and debug_mode:
            print(f"DEBUG: NYCModernBikeShareRecord validation failed - missing columns: {missing_columns}")
            print(f"DEBUG: Available columns: {list(df.columns)}")
        return not missing_columns
```

After deletion, the class goes directly from the `_required_columns` list (ending at line 113) to the `to_dataframe` method (previously at line 126). Add a blank line between them for readability.

**Step 2c: Check whether `os` import can be removed**

After deleting both `validate_schema` methods, search the rest of `nyc_bike.py` for any remaining use of `os`. The `to_dataframe` methods do NOT use `os`. However, `os` is imported at line 7:

```python
import os
```

**Action:** Remove `import os` from line 7 -- it is no longer used in this file.

**Resulting `data_models/nyc_bike.py` (full file after changes):**

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
from data_models.base import BaseBikeShareRecord
import re

@dataclass
class NYCLegacyBikeShareRecord(BaseBikeShareRecord):
    tripduration: int
    bikeid: str
    starttime: str
    stoptime: str
    start_station_id: str
    start_station_name: str
    start_station_latitude: float
    start_station_longitude: float
    end_station_id: str
    end_station_name: str
    end_station_latitude: float
    end_station_longitude: float
    usertype: str
    birth_year: Optional[int]
    gender: Optional[int]
    source_file: str

    staging_table = "raw_nyc_legacy"
    s3_prefix = "nyc_csv/"

    # Store required columns for detailed validation
    _required_columns = [
        "tripduration",
        "starttime",
        "stoptime",
        "start station id",
        "start station name",
        "start station latitude",
        "start station longitude",
        "end station id",
        "end station name",
        "end station latitude",
        "end station longitude",
        "bikeid",
        "usertype",
        "birth year",
        "gender"
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        # Only rename columns that have different names in source
        df = df.rename(columns={
            "start station id": "start_station_id",
            "start station name": "start_station_name",
            "start station latitude": "start_station_latitude",
            "start station longitude": "start_station_longitude",
            "end station id": "end_station_id",
            "end station name": "end_station_name",
            "end station latitude": "end_station_latitude",
            "end station longitude": "end_station_longitude",
            "birth year": "birth_year",
        })
        df["source_file"] = source_file
        return df[list(cls.__dataclass_fields__.keys())]

@dataclass
class NYCModernBikeShareRecord(BaseBikeShareRecord):
    ride_id: str
    rideable_type: str
    started_at: str
    ended_at: str
    start_station_id: str
    start_station_name: str
    end_station_id: str
    end_station_name: str
    start_lat: float
    start_lng: float
    end_lat: float
    end_lng: float
    member_casual: str
    source_file: str

    staging_table = "raw_nyc_modern"
    s3_prefix = "nyc_csv/"

    # Store required columns for detailed validation
    _required_columns = [
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual"
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        # No column renames needed - modern schema uses correct names
        df["source_file"] = source_file

        # Ensure station IDs are treated as strings (handle alphanumeric values)
        for col in ["start_station_id", "end_station_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Handle potential data quality issues in coordinates
        for col in ["start_lat", "start_lng", "end_lat", "end_lng"]:
            if col in df.columns:
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df[list(cls.__dataclass_fields__.keys())]
```

### Change 3: `data_models/london_bike.py` -- Remove duplicate `validate_schema` methods

**Step 3a: Remove `LondonLegacyBikeShareRecord.validate_schema` (lines 39-48)**

Delete these 10 lines entirely:

```python
    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns for legacy London format."""
        debug_mode = os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG') == '1'

        missing_columns = [col for col in cls._required_columns if col not in df.columns]
        if missing_columns and debug_mode:
            print(f"DEBUG: LondonLegacyBikeShareRecord validation failed - missing columns: {missing_columns}")
            print(f"DEBUG: Available columns: {list(df.columns)}")
        return not missing_columns
```

After deletion, the class goes directly from the `_required_columns` list (ending at line 37) to the `to_dataframe` method (previously at line 50). Add a blank line between them for readability.

**Step 3b: Remove `LondonModernBikeShareRecord.validate_schema` (lines 100-109)**

Delete these 10 lines entirely:

```python
    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns for modern London format."""
        debug_mode = os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG') == '1'

        missing_columns = [col for col in cls._required_columns if col not in df.columns]
        if missing_columns and debug_mode:
            print(f"DEBUG: LondonModernBikeShareRecord validation failed - missing columns: {missing_columns}")
            print(f"DEBUG: Available columns: {list(df.columns)}")
        return not missing_columns
```

After deletion, the class goes directly from the `_required_columns` list (ending at line 98) to the `to_dataframe` method (previously at line 111). Add a blank line between them for readability.

**Step 3c: Check whether `os` import can be removed**

After deleting both `validate_schema` methods, search the rest of `london_bike.py` for any remaining use of `os`. The `to_dataframe` methods do NOT use `os`. However, `os` is imported at line 7:

```python
import os
```

**Action:** Remove `import os` from line 7 -- it is no longer used in this file.

**Resulting `data_models/london_bike.py` (full file after changes):**

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
from data_models.base import BaseBikeShareRecord
import re

@dataclass
class LondonLegacyBikeShareRecord(BaseBikeShareRecord):
    """Model for London bike share data from 2018-2020 (legacy schema)."""
    rental_id: str
    bike_id: str
    start_date: datetime
    end_date: datetime
    duration: int
    start_station_id: str
    start_station_name: str
    end_station_id: str
    end_station_name: str
    source_file: str

    staging_table = "raw_london_legacy"
    s3_prefix = "london_csv/"

    # Store required columns for detailed validation
    _required_columns = [
        "Rental Id",
        "Bike Id",
        "Start Date",
        "End Date",
        "StartStation Id",
        "StartStation Name",
        "EndStation Id",
        "EndStation Name",
        "Duration"
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        df = df.rename(columns={
            "Rental Id": "rental_id",
            "Bike Id": "bike_id",
            "Start Date": "start_date",
            "End Date": "end_date",
            "Duration": "duration",
            "StartStation Id": "start_station_id",
            "StartStation Name": "start_station_name",
            "EndStation Id": "end_station_id",
            "EndStation Name": "end_station_name"
        })
        df["source_file"] = source_file
        for col in ["start_date", "end_date"]:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M").dt.strftime("%Y-%m-%d %H:%M:%S")
        return df[list(cls.__dataclass_fields__.keys())]

@dataclass
class LondonModernBikeShareRecord(BaseBikeShareRecord):
    """Model for London bike share data from 2021+ (modern schema)."""
    number: str
    bike_number: str
    bike_model: str
    start_date: datetime
    end_date: datetime
    total_duration: str
    total_duration_ms: int  # This will be stored as BIGINT in PostgreSQL
    start_station_number: str
    start_station: str
    end_station_number: str
    end_station: str
    source_file: str

    staging_table = "raw_london_modern"
    s3_prefix = "london_csv/"

    # Store required columns for detailed validation
    _required_columns = [
        "Number",
        "Bike model",
        "Start date",
        "End date",
        "Start station number",
        "Start station",
        "End station number",
        "End station",
        "Total duration"
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        df = df.rename(columns={
            "Number": "number",
            "Bike number": "bike_number",
            "Bike model": "bike_model",
            "Start date": "start_date",
            "End date": "end_date",
            "Total duration": "total_duration",
            "Total duration (ms)": "total_duration_ms",
            "Start station number": "start_station_number",
            "Start station": "start_station",
            "End station number": "end_station_number",
            "End station": "end_station"
        })
        df["source_file"] = source_file

        # Handle date format variations
        for col in ["start_date", "end_date"]:
            try:
                # Try modern format first (YYYY-MM-DD HH:MM)
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M")
            except ValueError:
                try:
                    # Try legacy format (DD/MM/YYYY HH:MM)
                    df[col] = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M")
                except ValueError:
                    # Try mixed format with dayfirst=True for British dates
                    df[col] = pd.to_datetime(df[col], dayfirst=True)

            # Convert to standard format
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Ensure bike_number is treated as string (handle non-numeric values)
        if "bike_number" in df.columns:
            df["bike_number"] = df["bike_number"].astype(str)

        return df[list(cls.__dataclass_fields__.keys())]
```

---

## Verification Checklist

Run each of these commands and confirm the expected outcome:

### 1. Full test suite passes

```bash
python -m pytest tests/ -v
```

All tests should pass. If any test explicitly imports `validate_schema` from a subclass, it will still resolve because Python's MRO (method resolution order) finds it on the base class.

### 2. Schema validation integration tests pass

```bash
python -m pytest tests/test_data_models_integration.py -v
```

This file contains 10 tests that exercise `validate_schema` and `to_dataframe` for all 4 models. Every test should pass identically to before.

### 3. Smoke test: validation returns False for empty DataFrame

```bash
python -c "from data_models.nyc_bike import NYCLegacyBikeShareRecord; import pandas as pd; print(NYCLegacyBikeShareRecord.validate_schema(pd.DataFrame()))"
```

Expected output: `False`

### 4. Smoke test: validation returns True for valid DataFrame

```bash
python -c "
from data_models.nyc_bike import NYCLegacyBikeShareRecord
import pandas as pd
cols = NYCLegacyBikeShareRecord._required_columns
df = pd.DataFrame(columns=cols)
print(NYCLegacyBikeShareRecord.validate_schema(df))
"
```

Expected output: `True`

### 5. Smoke test: DEBUG logging shows diagnostic output

```bash
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from data_models.nyc_bike import NYCLegacyBikeShareRecord
import pandas as pd
NYCLegacyBikeShareRecord.validate_schema(pd.DataFrame())
"
```

Expected output should include a line like:
```
DEBUG:data_models.base:NYCLegacyBikeShareRecord validation failed - missing columns: ['tripduration', 'starttime', ...]. Available: []
```

### 6. Verify all 4 models resolve `validate_schema` from the base class

```bash
python -c "
from data_models import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord, LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from data_models.base import BaseBikeShareRecord
for cls in [NYCLegacyBikeShareRecord, NYCModernBikeShareRecord, LondonLegacyBikeShareRecord, LondonModernBikeShareRecord]:
    assert 'validate_schema' not in cls.__dict__, f'{cls.__name__} still defines its own validate_schema'
    assert cls.validate_schema == BaseBikeShareRecord.validate_schema, f'{cls.__name__} does not inherit from base'
print('All 4 models correctly inherit validate_schema from BaseBikeShareRecord')
"
```

---

## What NOT to Do

1. **Do NOT modify `to_dataframe` methods.** They are correctly different per subclass (different column renames, type coercions, date parsing). Only `validate_schema` is being consolidated.

2. **Do NOT remove `_required_columns` from any subclass.** The base class method relies on each subclass defining its own `_required_columns` list. The `_required_columns: list = []` on the base class is only a safe default.

3. **Do NOT remove the `os` import from subclass files UNTIL you verify it is unused.** After Phase 01 cleanup, confirm `os` is not referenced anywhere else in `nyc_bike.py` or `london_bike.py` before removing the import. (As of the current codebase state described above, `os` is only used in `validate_schema` in both files.)

4. **Do NOT remove the `re` import from `nyc_bike.py` or `london_bike.py`.** Even though `re` appears unused in the current code, it may be used after Phase 01 changes or could be needed for future work. Leave it unless you can confirm it is unused.

5. **Do NOT change the return value semantics.** The method must continue to return `bool` -- `True` if all required columns are present, `False` otherwise.

6. **Do NOT remove the unused imports `sys`, `datetime`, or `numpy` from `base.py`.** Those are pre-existing and may be used after Phase 01. Clean them up in a separate phase if desired.

7. **Do NOT add the `EXTRACTED_FILE_MANAGER_DEBUG` environment variable check to the new base class method.** The whole point is to replace that mechanism with standard Python logging.

---

## Changelog Entry

Add this to `CHANGELOG.md` under `[Unreleased]`:

```markdown
### Changed
- **Data Models Base Class** - Consolidated duplicate `validate_schema` methods into `BaseBikeShareRecord`
  - Replaced 4 identical subclass implementations with a single base class method
  - Replaced `EXTRACTED_FILE_MANAGER_DEBUG` env-var-gated `print()` calls with `logging.debug()`
  - Subclasses now inherit validation behavior via `_required_columns` class attribute
```
