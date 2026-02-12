# Phase 05: Dashboard Hardening

**Status:** ✅ COMPLETE
**Started:** 2026-02-11
**Completed:** 2026-02-11
**PR:** #31

**PR Title:** `fix(dashboard): parameterize queries, decompose into functions, consolidate patterns`
**Risk Level:** Medium
**Estimated Effort:** Large (4-6 hours)
**Dependencies:** None (touches only `dashboard/` and `streamlit_data_manager/`)
**Blocks:** Phase 09 (test coverage)

---

## Summary

The Streamlit dashboard (`dashboard/app.py`) has three categories of tech debt:

1. **SQL injection risk** -- User-controlled values (page names, date strings) are interpolated into DuckDB SQL via f-strings. While the current UI constrains inputs to a radio button and date picker, this is fragile: any future code change that accepts free-text input would create a real injection vector.
2. **Repeated patterns** -- The session state initialization is 14 lines of boilerplate. The date range queries are 4 near-identical try/except blocks. Each can be collapsed into a helper.
3. **Hardcoded S3 bucket** -- `streamlit_data_manager/parquet_file_manager.py` hardcodes the bucket name instead of reading from the environment.

This PR parameterizes all WHERE-clause values in SQL queries, extracts helpers for session state and date range resolution, adds a `run_query_params` function, and makes the S3 bucket configurable.

---

## Files Modified

| # | File | Action |
|---|------|--------|
| 1 | `dashboard/app.py` | Parameterize WHERE-clause values; extract `run_query_params`; extract `get_date_range` helper; consolidate session state defaults |
| 2 | `streamlit_data_manager/parquet_file_manager.py` | Replace hardcoded S3 bucket with `os.environ.get()` |

---

## Problem 1: SQL Injection Risk in WHERE Clauses

### Current pattern (multiple locations in `dashboard/app.py`)

User-controlled values are interpolated via f-strings directly into SQL:

**Line 86** -- Date values interpolated into WHERE clause:
```python
date_query = f"SELECT MIN(date) as min_date FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics.parquet')}' WHERE date >= '{dashboard_min_date}' AND date <= '{comparison_max_date}'"
```

**Lines 266, 282-283, 298-299, 325, 337, 372, 399, 409, 421-422** -- `applied_page.lower()` interpolated into WHERE clause:
```python
WHERE location = '{applied_page.lower()}'
```

**Lines 168, 210-211, 230, 242, 267, 283, 299, 327, 339, 373, 446-447, 457-458, 484** -- `date_filter` string interpolated (constructed from `applied_start_date` and `applied_end_date`):
```python
def date_filter_sql(start_date, end_date):
    return f"date BETWEEN '{max(start_date, dashboard_min_date)}' AND '{min(end_date, dashboard_max_date)}'"
```

### Important distinction: what IS and IS NOT safe

The Parquet file path references like `FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics.parquet')}'` are **safe** because `DATA_DIR` is a constant derived from `os.path.dirname()`. These are NOT user inputs and do NOT need parameterization. DuckDB requires single-quoted string literals for file paths and does not support parameters in the `FROM` clause.

Only the **WHERE clause values** need parameterization: location strings, date strings, and year values.

### Fix: Use DuckDB parameterized queries

DuckDB supports `$1, $2, ...` positional parameters via `connection.execute(query, [param1, param2, ...])`.

**Add a `run_query_params` helper** alongside the existing `run_query` function.

**BEFORE** (line 26-27):
```python
def run_query(query):
    return DUCKDB_CONN.execute(query).fetchdf()
```

**AFTER** (lines 26-30):
```python
def run_query(query):
    return DUCKDB_CONN.execute(query).fetchdf()

def run_query_params(query, params):
    return DUCKDB_CONN.execute(query, params).fetchdf()
```

### Conversion examples

Below are representative examples showing how to convert each query pattern. Apply the same approach to every query in the file that interpolates user-controlled values.

#### Example A: Location filter (lines 263-269)

**BEFORE:**
```python
total_rides_query = f"""
SELECT SUM(metric_value) as total_rides
FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics_long.parquet')}'
WHERE location = '{applied_page.lower()}'
  AND {date_filter}
  AND metric_name = 'total_rides'
"""
try:
    total_rides_result = run_query(total_rides_query)
```

**AFTER:**
```python
total_rides_query = f"""
SELECT SUM(metric_value) as total_rides
FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics_long.parquet')}'
WHERE location = $1
  AND date BETWEEN $2 AND $3
  AND metric_name = 'total_rides'
"""
try:
    total_rides_result = run_query_params(total_rides_query, [
        applied_page.lower(),
        str(max(applied_start_date, dashboard_min_date)),
        str(min(applied_end_date, dashboard_max_date)),
    ])
```

Note: `metric_name = 'total_rides'` is a **string literal constant**, not user input. It does NOT need parameterization. Only `location`, `date BETWEEN`, and similar user-derived values need parameters.

#### Example B: Date range query (lines 84-93)

**BEFORE:**
```python
if st.session_state['applied_page'] == "Comparison":
    comparison_max_date = min(nyc_max_date, london_max_date)
    date_query = f"SELECT MIN(date) as min_date FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics.parquet')}' WHERE date >= '{dashboard_min_date}' AND date <= '{comparison_max_date}'"
    try:
        date_df = run_query(date_query)
```

**AFTER:**
```python
if st.session_state['applied_page'] == "Comparison":
    comparison_max_date = min(nyc_max_date, london_max_date)
    date_query = f"SELECT MIN(date) as min_date FROM '{os.path.join(DATA_DIR, 'mart_daily_metrics.parquet')}' WHERE date >= $1 AND date <= $2"
    try:
        date_df = run_query_params(date_query, [str(dashboard_min_date), str(comparison_max_date)])
```

#### Example C: Hourly patterns query (line 399)

**BEFORE:**
```python
hour_query = f"SELECT hour_of_day, ride_count FROM '{os.path.join(DATA_DIR, 'mart_hourly_patterns.parquet')}' WHERE location = '{applied_page.lower()}' ORDER BY hour_of_day"
try:
    hour_df = run_query(hour_query)
```

**AFTER:**
```python
hour_query = f"SELECT hour_of_day, ride_count FROM '{os.path.join(DATA_DIR, 'mart_hourly_patterns.parquet')}' WHERE location = $1 ORDER BY hour_of_day"
try:
    hour_df = run_query_params(hour_query, [applied_page.lower()])
```

#### Example D: Station growth with EXTRACT year (lines 418-424)

**BEFORE:**
```python
station_query = f"""
SELECT year, station_count as metric_value
FROM '{os.path.join(DATA_DIR, 'mart_station_growth.parquet')}'
WHERE location = '{applied_page.lower()}'
AND year BETWEEN EXTRACT(YEAR FROM DATE '{applied_start_date}') AND EXTRACT(YEAR FROM DATE '{applied_end_date}')
ORDER BY year
"""
```

**AFTER:**
```python
station_query = f"""
SELECT year, station_count as metric_value
FROM '{os.path.join(DATA_DIR, 'mart_station_growth.parquet')}'
WHERE location = $1
AND year BETWEEN EXTRACT(YEAR FROM $2::DATE) AND EXTRACT(YEAR FROM $3::DATE)
ORDER BY year
"""
try:
    station_df = run_query_params(station_query, [
        applied_page.lower(),
        str(applied_start_date),
        str(applied_end_date),
    ])
```

Note: DuckDB allows `$2::DATE` to cast the parameter to a DATE type.

#### Example E: The `date_filter_sql` helper (line 167-168)

**BEFORE:**
```python
def date_filter_sql(start_date, end_date):
    return f"date BETWEEN '{max(start_date, dashboard_min_date)}' AND '{min(end_date, dashboard_max_date)}'"
date_filter = date_filter_sql(applied_start_date, applied_end_date)
```

**AFTER -- Remove `date_filter_sql` entirely.** Instead, compute the clamped date values once and use them as parameters in every query:

```python
clamped_start = str(max(applied_start_date, dashboard_min_date))
clamped_end = str(min(applied_end_date, dashboard_max_date))
```

Then in each query, use `date BETWEEN $N AND $M` and pass `clamped_start` and `clamped_end` as parameters (numbering depends on how many other params the query uses).

### Full list of queries to parameterize

Each of the following locations in `dashboard/app.py` needs its f-string WHERE-clause values converted to `$N` parameters. The parquet file paths in the `FROM` clause stay as f-strings.

| Line(s) | Query variable | Values to parameterize |
|----------|---------------|----------------------|
| 61 | `nyc_max_date_query` | `'nyc'` is a constant literal -- safe, no change needed |
| 62 | `london_max_date_query` | `'london'` is a constant literal -- safe, no change needed |
| 86 | `date_query` (Comparison) | `dashboard_min_date`, `comparison_max_date` |
| 95 | `date_query` (NYC) | `'nyc'`, `dashboard_min_date`, `nyc_max_date` |
| 104 | `date_query` (London) | `'london'`, `dashboard_min_date`, `london_max_date` |
| 167-168 | `date_filter_sql` / `date_filter` | Replace entirely; use `clamped_start` / `clamped_end` as params |
| 181-184 | `year_query` | `applied_start_date`, `applied_end_date` |
| 192-197 | `pop_query` | `latest_year` (integer) |
| 207-211 | `rides_query` | `clamped_start`, `clamped_end` |
| 226-231 | `nyc_duration_query` | `'nyc'`, `clamped_start`, `clamped_end` |
| 238-243 | `london_duration_query` | `'london'`, `clamped_start`, `clamped_end` |
| 263-269 | `total_rides_query` | `applied_page.lower()`, `clamped_start`, `clamped_end` |
| 277-287 | `avg_daily_query` | `applied_page.lower()`, `clamped_start`, `clamped_end` |
| 295-300 | `avg_duration_query` | `applied_page.lower()`, `clamped_start`, `clamped_end` |
| 322-330 | `rides_trend_query` (avg) | `applied_page.lower()`, `clamped_start`, `clamped_end` |
| 334-342 | `rides_trend_query` (total) | `applied_page.lower()`, `clamped_start`, `clamped_end` |
| 365-377 | `duration_trend_query` | `applied_page.lower()`, `clamped_start`, `clamped_end` |
| 399 | `hour_query` | `applied_page.lower()` |
| 409 | `member_query` | `applied_start_date`, `applied_end_date` |
| 418-424 | `station_query` (single city) | `applied_page.lower()`, `applied_start_date`, `applied_end_date` |
| 443-450 | `comparison_query` (overall) | `clamped_start`, `clamped_end` |
| 454-461 | `comparison_query` (per capita) | `clamped_start`, `clamped_end` |
| 479-488 | `duration_query` (comparison) | `clamped_start`, `clamped_end` |
| 504-509 | `station_query` (comparison) | `applied_start_date`, `applied_end_date` |

---

## Problem 2: Session State Boilerplate

### Current pattern (lines 43-58)

```python
def set_default_state():
    if 'pending_page' not in st.session_state:
        st.session_state['pending_page'] = 'NYC'
    if 'pending_start_date' not in st.session_state:
        st.session_state['pending_start_date'] = dashboard_min_date
    if 'pending_end_date' not in st.session_state:
        st.session_state['pending_end_date'] = dashboard_max_date
    if 'applied_page' not in st.session_state:
        st.session_state['applied_page'] = 'NYC'
    if 'applied_start_date' not in st.session_state:
        st.session_state['applied_start_date'] = dashboard_min_date
    if 'applied_end_date' not in st.session_state:
        st.session_state['applied_end_date'] = dashboard_max_date
    if 'date_filter_applied' not in st.session_state:
        st.session_state['date_filter_applied'] = False
set_default_state()
```

### AFTER

```python
SESSION_DEFAULTS = {
    'pending_page': 'NYC',
    'pending_start_date': dashboard_min_date,
    'pending_end_date': dashboard_max_date,
    'applied_page': 'NYC',
    'applied_start_date': dashboard_min_date,
    'applied_end_date': dashboard_max_date,
    'date_filter_applied': False,
}

def set_default_state():
    for key, default in SESSION_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default

set_default_state()
```

This reduces 14 lines to 7 lines and makes it easy to add new state keys in the future.

**Important:** Keep the key names exactly the same (`'pending_page'`, `'pending_start_date'`, etc.). Other parts of the code reference them by name.

---

## Problem 3: Repeated Date Range Queries

### Current pattern (lines 84-114)

Four near-identical if/elif blocks, each running a query and handling exceptions:

```python
if st.session_state['applied_page'] == "Comparison":
    comparison_max_date = min(nyc_max_date, london_max_date)
    date_query = f"SELECT MIN(date) as min_date FROM '...' WHERE date >= '{dashboard_min_date}' AND date <= '{comparison_max_date}'"
    try:
        date_df = run_query(date_query)
        min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date) if not date_df.empty else dashboard_min_date
        max_date = comparison_max_date
    except Exception as e:
        min_date = dashboard_min_date
        max_date = comparison_max_date
elif st.session_state['applied_page'] == "NYC":
    date_query = f"SELECT MIN(date) as min_date FROM '...' WHERE location = 'nyc' AND date >= '{dashboard_min_date}' AND date <= '{nyc_max_date}'"
    try:
        date_df = run_query(date_query)
        min_date = max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date) if not date_df.empty else dashboard_min_date
        max_date = nyc_max_date
    except Exception as e:
        min_date = dashboard_min_date
        max_date = nyc_max_date
elif st.session_state['applied_page'] == "London":
    # ... same pattern ...
else:
    min_date = dashboard_min_date
    max_date = dashboard_max_date
```

### AFTER -- Extract a `get_date_range` helper

Add this function near the top of the file (after the `run_query_params` definition):

```python
def get_date_range(location: str, upper_bound_date) -> tuple:
    """Get the min/max date range for a location from the daily metrics mart.

    Args:
        location: 'nyc', 'london', or 'comparison'
        upper_bound_date: The maximum date to constrain the range

    Returns:
        Tuple of (min_date, max_date) as datetime.date objects
    """
    parquet_path = os.path.join(DATA_DIR, 'mart_daily_metrics.parquet')
    if location == 'comparison':
        query = f"SELECT MIN(date) as min_date FROM '{parquet_path}' WHERE date >= $1 AND date <= $2"
        params = [str(dashboard_min_date), str(upper_bound_date)]
    else:
        query = f"SELECT MIN(date) as min_date FROM '{parquet_path}' WHERE location = $1 AND date >= $2 AND date <= $3"
        params = [location, str(dashboard_min_date), str(upper_bound_date)]
    try:
        date_df = run_query_params(query, params)
        resolved_min = (
            max(pd.to_datetime(date_df['min_date'][0]).date(), dashboard_min_date)
            if not date_df.empty and date_df['min_date'][0] is not None
            else dashboard_min_date
        )
        return resolved_min, upper_bound_date
    except Exception:
        return dashboard_min_date, upper_bound_date
```

Then replace the 30-line if/elif chain with:

```python
applied = st.session_state['applied_page']
if applied == "Comparison":
    comparison_max_date = min(nyc_max_date, london_max_date)
    min_date, max_date = get_date_range('comparison', comparison_max_date)
elif applied == "NYC":
    min_date, max_date = get_date_range('nyc', nyc_max_date)
elif applied == "London":
    min_date, max_date = get_date_range('london', london_max_date)
else:
    min_date, max_date = dashboard_min_date, dashboard_max_date
```

This reduces 30+ lines to 8 lines and uses parameterized queries.

---

## Problem 4: Hardcoded S3 Bucket in `parquet_file_manager.py`

### Current code (`streamlit_data_manager/parquet_file_manager.py`, line 12)

```python
S3_BUCKET = "city-cycles-data-ctr37"
```

### AFTER

```python
S3_BUCKET = os.environ.get("S3_BUCKET", "city-cycles-data-ctr37")
```

The file already imports `os` (line 10), so no new import is needed. The default value preserves backward compatibility: if the env var is not set, the existing bucket name is used.

Note: The file does NOT currently import `dotenv`. This is fine because:
- When called from `dashboard/app.py`, `load_dotenv()` is already called at line 8 of `app.py`.
- When called standalone, the environment variable should be set by the calling process.

Do NOT add a `load_dotenv()` call here -- it would create a side effect in a utility module.

---

## Full Conversion Reference

Below is a **complete before/after** for the query helper section, showing how the `date_filter_sql` function is removed and replaced with clamped date parameters.

### BEFORE (lines 166-169)

```python
if st.session_state.get('date_filter_applied', False) and applied_start_date and applied_end_date:
    def date_filter_sql(start_date, end_date):
        return f"date BETWEEN '{max(start_date, dashboard_min_date)}' AND '{min(end_date, dashboard_max_date)}'"
    date_filter = date_filter_sql(applied_start_date, applied_end_date)
```

### AFTER

```python
if st.session_state.get('date_filter_applied', False) and applied_start_date and applied_end_date:
    clamped_start = str(max(applied_start_date, dashboard_min_date))
    clamped_end = str(min(applied_end_date, dashboard_max_date))
```

Then every query that previously used `{date_filter}` now uses `date BETWEEN $N AND $M` with `clamped_start` and `clamped_end` passed as parameters.

---

## Verification Checklist

### 1. Dashboard loads and all pages render

```bash
cd /Users/chris/Projects/city-cycles
streamlit run dashboard/app.py
```

- Navigate to NYC page -- verify metrics and charts display
- Navigate to London page -- verify metrics and charts display
- Navigate to Comparison page -- verify comparative metrics and charts display

### 2. Date filter works correctly

- Set a custom date range (e.g., 2020-01-01 to 2022-12-31)
- Click "Apply Date Filter"
- Verify charts update to reflect the filtered range
- Click "Reset Dashboard"
- Verify dashboard returns to full date range

### 3. Verify no SQL injection is possible

After the changes, try this manual test:

```python
# In a Python shell, verify that parameterized queries escape values correctly:
import duckdb
conn = duckdb.connect(':memory:')
# This should NOT cause an error or unexpected behavior:
conn.execute("SELECT $1 as test", ["'; DROP TABLE test; --"]).fetchdf()
# Should return a DataFrame with one column 'test' containing the string literal
```

### 4. Verify parquet_file_manager respects environment

```bash
S3_BUCKET=test-bucket python -c "from streamlit_data_manager.parquet_file_manager import S3_BUCKET; print(S3_BUCKET)"
```

Expected output: `test-bucket`

```bash
python -c "from streamlit_data_manager.parquet_file_manager import S3_BUCKET; print(S3_BUCKET)"
```

Expected output: `city-cycles-data-ctr37` (default fallback)

### 5. Run existing tests

```bash
python -m pytest tests/ -v
```

All tests should pass. There are no existing dashboard-specific tests, but the refactor should not break any other module.

---

## What NOT to Do

1. **Do NOT parameterize Parquet file paths in the FROM clause.** DuckDB requires the file path as a string literal in `FROM '...'`. These paths are derived from constants (`DATA_DIR`), not user input.

2. **Do NOT change the Streamlit page layout or visual design.** This PR is about query safety and code structure. The UI should look identical before and after.

3. **Do NOT remove any chart or data display.** Every chart, metric, and data table must remain.

4. **Do NOT modify the Plotly chart configurations.** Colors, titles, axis labels, etc. remain unchanged.

5. **Do NOT change how session state keys are named.** The keys `'pending_page'`, `'applied_page'`, `'pending_start_date'`, etc. are referenced throughout the file. Renaming them would break the UI.

6. **Do NOT add `load_dotenv()` to `parquet_file_manager.py`.** The calling code (`dashboard/app.py`) already loads the `.env` file.

7. **Do NOT convert the constant string literals in queries (like `'total_rides'`, `'nyc'`, `'london'` when hardcoded) to parameters.** Only convert values that originate from user input or session state.

8. **Do NOT change the DuckDB connection setup.** The `duckdb.connect(database=':memory:')` call on line 23 stays as-is.

---

## Changelog Entry

Add this to `CHANGELOG.md` under `[Unreleased]`:

```markdown
### Fixed
- **SQL Injection in Dashboard** - Parameterized all user-controlled values in DuckDB queries
  - Replaced f-string interpolation with `$1, $2, ...` positional parameters
  - Added `run_query_params` helper for parameterized query execution
  - Location, date, and year values now passed safely as query parameters

### Changed
- **Dashboard Code Quality** - Decomposed repeated patterns into helper functions
  - Extracted `get_date_range()` helper, reducing 30 lines of duplicate code to 8
  - Consolidated session state initialization into dict-driven `set_default_state()`
  - Replaced `date_filter_sql()` f-string helper with clamped date parameter values

### Fixed
- **Hardcoded S3 Bucket** - `streamlit_data_manager/parquet_file_manager.py` now reads `S3_BUCKET` from environment variable with fallback to default
```
