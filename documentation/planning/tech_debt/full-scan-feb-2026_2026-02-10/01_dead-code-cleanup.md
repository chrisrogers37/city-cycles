# Phase 01: Dead Code Cleanup

**Status:** ✅ COMPLETE
**Started:** 2026-02-11
**Completed:** 2026-02-11
**PR:** #26

**PR Title:** `chore: remove dead code, unused imports, and placeholder files`
**Risk Level:** None
**Estimated Effort:** Small (1-2 hours)
**Dependencies:** None
**Blocks:** Phase 04 (data models refactor touches same files)

---

## Summary

Remove unused imports, dead variables, and placeholder files across the Python codebase. Every change in this PR is a pure deletion with zero logic modifications. The test suite must remain fully passing.

---

## Files Modified

| # | File | Action |
|---|------|--------|
| 1 | `data_models/base.py` | Remove 2 unused imports |
| 2 | `data_models/nyc_bike.py` | Remove 4 unused imports |
| 3 | `data_models/london_bike.py` | Remove 2 unused imports |
| 4 | `db_duckdb/duckdb_manager.py` | Remove 1 unused import |
| 5 | `extracted_file_manager/manager.py` | Remove 1 unused import, 1 unused import line, 1 redundant import |
| 6 | `extraction/weather.py` | Delete entire placeholder file |
| 7 | `extraction/london.py` | Remove 1 unused variable |

---

## Change 1: `data_models/base.py`

**What:** Remove unused imports `sys` (line 2) and `numpy as np` (line 6). Neither `sys` nor `np` appears anywhere else in the file.

**BEFORE (lines 1-6):**
```python
import os
import sys
import pandas as pd
from typing import Type, List
from datetime import datetime
import numpy as np
```

**AFTER (lines 1-4):**
```python
import os
import pandas as pd
from typing import Type, List
from datetime import datetime
```

**Imports kept and why:**
- `os` -- Used by subclasses that inherit from this module's registry pattern (keep for safety, though not directly used in base.py itself -- removing it is out of scope for this PR)
- `pandas as pd` -- Used in `validate_schema()` and `to_dataframe()` type hints (`pd.DataFrame`)
- `Type, List` from typing -- Used in `_registry: List[Type['BaseBikeShareRecord']]`
- `datetime` -- Used in downstream subclass context (keep for safety)

---

## Change 2: `data_models/nyc_bike.py`

**What:** Remove 4 unused imports: `datetime` (line 2), `Dict` and `Any` from typing (line 3), and `re` (line 6). None of these symbols are referenced anywhere in the file body. The `datetime` type is NOT used as a field type annotation in NYC models (all fields use `str`, `int`, `float`, or `Optional`).

**BEFORE (lines 1-7):**
```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
from data_models.base import BaseBikeShareRecord
import re
import os
```

**AFTER (lines 1-5):**
```python
from dataclasses import dataclass
from typing import Optional
import pandas as pd
from data_models.base import BaseBikeShareRecord
import os
```

**Imports kept and why:**
- `dataclass` -- Used as decorator on `NYCLegacyBikeShareRecord` and `NYCModernBikeShareRecord`
- `Optional` -- Used in field type hints (`Optional[int]` for `birth_year`, `gender`)
- `pandas as pd` -- Used in `validate_schema(cls, df: pd.DataFrame)` and `to_dataframe()`
- `BaseBikeShareRecord` -- Parent class for both record models
- `os` -- Used for `os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG')` in `validate_schema()`

---

## Change 3: `data_models/london_bike.py`

**What:** Remove 2 unused imports: `Dict` and `Any` from typing (line 3), and `re` (line 6). **Do NOT remove `datetime`** -- it IS used as a type annotation on `start_date: datetime` and `end_date: datetime` fields in both `LondonLegacyBikeShareRecord` and `LondonModernBikeShareRecord`.

**BEFORE (lines 1-7):**
```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
from data_models.base import BaseBikeShareRecord
import re
import os
```

**AFTER (lines 1-5):**
```python
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from data_models.base import BaseBikeShareRecord
import os
```

**Imports kept and why:**
- `dataclass` -- Used as decorator on both London record classes
- `datetime` -- Used as field type: `start_date: datetime` and `end_date: datetime` (lines 14-15 and 74-75 in current file)
- `pandas as pd` -- Used in method signatures
- `BaseBikeShareRecord` -- Parent class
- `os` -- Used for `os.environ.get('EXTRACTED_FILE_MANAGER_DEBUG')`

**IMPORTANT:** The user instructions say to remove `datetime` from this file. That is INCORRECT. `datetime` is actively used as a type annotation. Do NOT remove it. Only remove `Dict, Any` from the typing import and `re`.

---

## Change 4: `db_duckdb/duckdb_manager.py`

**What:** Remove unused `import boto3` (line 2). The `boto3` module is never referenced in this file -- S3 access is configured through DuckDB's built-in httpfs/s3 extensions using raw SQL `SET` commands.

**BEFORE (lines 1-6):**
```python
import duckdb
import boto3
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv
import logging
```

**AFTER (lines 1-5):**
```python
import duckdb
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv
import logging
```

**Imports kept and why:**
- `duckdb` -- Core database library, used throughout
- `os` -- Used for `os.makedirs()`, `os.environ.get()`
- `List, Dict, Optional` -- Used in method return types and parameters
- `load_dotenv` -- Used in `_setup_s3_access()`
- `logging` -- Used for `logger` throughout the class

---

## Change 5: `extracted_file_manager/manager.py`

Three separate sub-changes in this file.

### 5a: Remove unused `import pyarrow.csv as pv` (line 14)

**What:** The alias `pv` is never referenced anywhere in the file. PyArrow CSV reading is done through pandas, not through `pyarrow.csv` directly.

**BEFORE (lines 13-15):**
```python
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
```

**AFTER (lines 13-14):**
```python
import pyarrow as pa
import pyarrow.parquet as pq
```

### 5b: Remove unused filetree imports (line 26)

**What:** `ZipFile as ZipFileNode` and `walk_folder` are imported from `.filetree` but neither `ZipFileNode` nor `walk_folder` appears anywhere else in `manager.py`. The manager uses `zipfile.ZipFile` from the standard library directly (see lines 251, 330) rather than the custom filetree classes. Remove the entire import line.

**BEFORE (line 26):**
```python
from .filetree import ZipFile as ZipFileNode, walk_folder
```

**AFTER:**
(Delete the entire line. No replacement.)

### 5c: Remove redundant `import time` inside `_cleanup_memory()` (lines 127-128)

**What:** The `time` module is already imported at module level (line 18: `import time`). The local re-import inside `_cleanup_memory()` is redundant and confusing.

**BEFORE (`_cleanup_memory` method, around lines 123-128):**
```python
    def _cleanup_memory(self):
        """Force garbage collection and cleanup."""
        gc.collect()
        # Small delay to allow cleanup
        import time
        time.sleep(0.1)
```

**AFTER:**
```python
    def _cleanup_memory(self):
        """Force garbage collection and cleanup."""
        gc.collect()
        # Small delay to allow cleanup
        time.sleep(0.1)
```

---

## Change 6: `extraction/weather.py`

**What:** Delete this file entirely. It contains only a single comment:
```python
# NOT YET IMPLEMENTED: This module is a placeholder for future weather data ingestion logic.
```

There is no code, no imports, no classes, no functions. No other file in the codebase imports from `extraction.weather`. This placeholder adds noise to the codebase.

**Action:** `git rm extraction/weather.py`

**Verification before deleting:** Run a grep across the entire codebase to confirm nothing imports from this file:
```bash
grep -r "weather" --include="*.py" . | grep -v "__pycache__"
```
Expected: Only the file itself and possibly test references. If any import is found, do NOT delete the file.

---

## Change 7: `extraction/london.py`

**What:** Remove the unused variable `start_time = time.time()` on line 31. This variable is assigned but never read. The loop below it runs a fixed 30 iterations with `asyncio.sleep(1)` -- it does not check `start_time`.

**BEFORE (lines 30-32):**
```python
        # Scroll the #full-width-content container for up to 30 seconds
        start_time = time.time()
        for _ in range(30):
```

**AFTER (lines 30-31):**
```python
        # Scroll the #full-width-content container for up to 30 seconds
        for _ in range(30):
```

Also update the comment if you want to be precise (optional, but recommended since the comment references a time-based approach that was abandoned):

**Optional improved AFTER:**
```python
        # Scroll the #full-width-content container 30 times (once per second)
        for _ in range(30):
```

---

## What NOT To Do

- **Do NOT remove `os` from any file** -- it is used for `os.environ.get()`, `os.makedirs()`, `os.path.*`, etc.
- **Do NOT remove `from data_models.base import BaseBikeShareRecord`** from `manager.py` -- it is used in `_find_matching_model()`
- **Do NOT remove `datetime` from `london_bike.py`** -- it IS used as a field type annotation (the user instructions are wrong on this point; verify by checking lines 14-15 and 74-75)
- **Do NOT delete `extraction/__init__.py`** or any `__init__.py` files
- **Do NOT modify any logic or function bodies** beyond removing the specific dead imports/variables listed above
- **Do NOT remove any imports from files not listed in this plan**
- **Do NOT reorder the remaining imports** -- keep them in their current order to minimize diff noise

---

## Verification Checklist

Run ALL of the following after making changes. Every check must pass before opening the PR.

### 1. Test Suite
```bash
python -m pytest tests/ -v
```
Expected: 83 pass, 3 skip, 0 fail (86 total). No test should change status.

### 2. Import Smoke Tests
```bash
python -c "from data_models.base import BaseBikeShareRecord; print('base OK')"
python -c "from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord; print('nyc OK')"
python -c "from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord; print('london OK')"
python -c "from extracted_file_manager.manager import ExtractedFileManager; print('manager OK')"
python -c "from db_duckdb.duckdb_manager import DuckDBManager; print('duckdb OK')"
python -c "from extraction.london import list_london_csv_files; print('london extraction OK')"
```
All should print their "OK" message with no ImportError or other exceptions.

### 3. Verify weather.py Deletion Was Safe
```bash
grep -rn "weather" --include="*.py" . | grep -v "__pycache__" | grep -v "weather.py"
```
Expected: No results referencing `extraction.weather` as an import.

### 4. Verify No Remaining References to Removed Symbols
```bash
# Check that removed symbols are truly not used
grep -rn "ZipFileNode\|walk_folder" extracted_file_manager/manager.py
grep -rn "\bnp\b" data_models/base.py
grep -rn "\bsys\b" data_models/base.py
grep -rn "\bboto3\b" db_duckdb/duckdb_manager.py
grep -rn "\bpv\b" extracted_file_manager/manager.py
```
All should return only the import lines (which you already removed) or zero results.

### 5. Git Diff Review
```bash
git diff --stat
```
Verify that only the 7 files listed above are modified/deleted. No other files should appear in the diff.

---

## PR Checklist

- [ ] All 7 changes applied exactly as specified
- [ ] `python -m pytest tests/ -v` passes (83 pass, 3 skip)
- [ ] All 6 import smoke tests pass
- [ ] `git diff` shows only the 7 target files
- [ ] No logic changes -- only import/variable deletions
- [ ] CHANGELOG.md updated with entry under `[Unreleased]`

### CHANGELOG Entry
```markdown
### Technical Improvements
- **Dead Code Cleanup** - Removed unused imports, dead variables, and placeholder files
  - Removed unused `sys`, `numpy`, `boto3`, `pyarrow.csv`, `re`, `Dict`, `Any`, `datetime` imports across 5 files
  - Removed unused `ZipFileNode` and `walk_folder` filetree imports from file manager
  - Removed redundant local `import time` in `_cleanup_memory()`
  - Deleted empty placeholder `extraction/weather.py`
  - Removed dead variable `start_time` in `extraction/london.py`
```
