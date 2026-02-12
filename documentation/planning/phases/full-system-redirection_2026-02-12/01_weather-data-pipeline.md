# Phase 01: Weather Data Pipeline (Foundation)

**Status:** 🔧 IN PROGRESS
**Started:** 2026-02-12

## PR Title
feat: add weather data extraction pipeline with Open-Meteo integration

## Risk Level: Low
## Estimated Effort: 2-3 days
## Dependencies: None (this is the foundation phase)
## Unlocks: Phases 02, 03, 06

## Files Impact
| Action | File | Notes |
|--------|------|-------|
| CREATE | extraction/weather.py | Core extraction module |
| CREATE | data_models/weather.py | HourlyWeatherRecord model |
| CREATE | dbt_city_cycles/models/staging/stg_weather_hourly.sql | dbt staging model |
| CREATE | tests/test_weather_extraction.py | Weather extraction tests |
| MODIFY | data_models/base.py | Rename BaseBikeShareRecord → BaseDataRecord |
| MODIFY | data_models/nyc_bike.py | Update import to BaseDataRecord |
| MODIFY | data_models/london_bike.py | Update import to BaseDataRecord |
| MODIFY | data_models/__init__.py | Add HourlyWeatherRecord export |
| MODIFY | data_models/registry.py | Add HourlyWeatherRecord + update import |
| MODIFY | data_models/README.md | Update class name references |
| MODIFY | extracted_file_manager/manager.py | Update import to BaseDataRecord |
| MODIFY | extraction/__init__.py | Add weather function exports |
| MODIFY | db_duckdb/config/duckdb_config.py | Add raw_weather_hourly config |
| MODIFY | db_duckdb/operations.py | Add weather quality config |
| MODIFY | dbt_city_cycles/models/staging/sources.yml | Add weather source |
| MODIFY | dbt_city_cycles/models/staging/schema.yml | Add stg_weather_hourly schema |
| MODIFY | orchestrator/main.py | Integrate weather extraction |
| MODIFY | orchestrator/cli.py | Add weather_extraction stage |
| MODIFY | tests/conftest.py | Add weather fixture |
| MODIFY | tests/test_data_models_integration.py | Update BaseDataRecord refs |

## Context
This phase adds the complete weather data pipeline to City Cycles, enabling all downstream weather-ride correlation features. Weather data flows through the same architectural pattern as bike data: extraction -> S3 -> DuckDB -> dbt. The API choice is Open-Meteo (free, no API key, hourly data since 1940, professional-grade ERA5 reanalysis data).

**Prerequisite refactor:** This phase also renames `BaseBikeShareRecord` → `BaseDataRecord` in `data_models/base.py` and all consumers. The base class is a generic schema-validation mixin with no bike-specific logic — the old name was misleading, and weather data should not inherit from "BikeShareRecord".

---

# Weather Data Pipeline (Phase 01) -- Detailed Implementation Plan

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Open-Meteo API Reference](#2-open-meteo-api-reference)
3. [File-by-File Implementation Guide](#3-file-by-file-implementation-guide)
4. [Modifications to Existing Files](#4-modifications-to-existing-files)
5. [dbt Models](#5-dbt-models)
6. [Tests](#6-tests)
7. [Verification Checklist](#7-verification-checklist)
8. [What NOT To Do](#8-what-not-to-do)
9. [Implementation Sequence](#9-implementation-sequence)

---

## 1. Architecture Overview

The weather pipeline follows the same flow as existing bike data, but weather does NOT pass through `extracted_file_manager` (no ZIPs/CSVs to process -- we fetch structured JSON from an API and write Parquet directly).

```
extraction/weather.py          --> Fetch JSON from Open-Meteo, write Parquet to S3
    |
    v
db_duckdb/                     --> Load Parquet into raw_weather_hourly table
    |
    v
dbt_city_cycles/staging/       --> stg_weather_hourly.sql (clean + derive fields)
    |
    v
(Future phases: join with rides in intermediate/marts)
```

**S3 path convention** (matching existing pattern):
```
s3://city-cycles-data-ctr37/extracted_weather_parquet/nyc/*.parquet
s3://city-cycles-data-ctr37/extracted_weather_parquet/london/*.parquet
```

---

## 2. Open-Meteo API Reference

### Historical Weather API
- **Base URL:** `https://archive-api.open-meteo.com/v1/archive`
- **Parameters:**
  - `latitude` / `longitude` (required): WGS84 floats
  - `start_date` / `end_date` (required): `YYYY-MM-DD` format
  - `hourly` (required): comma-separated variable names
  - `timezone`: use `America/New_York` for NYC, `Europe/London` for London
  - `timeformat`: `iso8601` (default)
- **Rate limit:** 10,000 requests/day (free tier). No per-second throttle, but a 0.5s sleep between requests is courteous.
- **Max date range per request:** The API can return large ranges. For safety, chunk requests by year to keep response payloads manageable.

### Forecast API (for incremental/current)
- **Base URL:** `https://api.open-meteo.com/v1/forecast`
- **Parameters:** same as above but uses `forecast_days` (0-16) and `past_days` (0-92) instead of `start_date`/`end_date`.

### Hourly Variables to Request
```
temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,snow_depth,weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m
```

### Example Request
```
https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.0060&start_date=2023-01-01&end_date=2023-12-31&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,snow_depth,weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m&timezone=America/New_York
```

### Response Shape (JSON)
```json
{
  "latitude": 40.7,
  "longitude": -74.0,
  "timezone": "America/New_York",
  "hourly_units": {
    "time": "iso8601",
    "temperature_2m": "°C",
    ...
  },
  "hourly": {
    "time": ["2023-01-01T00:00", "2023-01-01T01:00", ...],
    "temperature_2m": [1.2, 0.8, ...],
    "relative_humidity_2m": [85, 87, ...],
    ...
  }
}
```

Sources:
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [Open-Meteo Forecast API](https://open-meteo.com/en/docs)

---

## 3. File-by-File Implementation Guide

### 3.1 NEW FILE: `/Users/chris/Projects/city-cycles/extraction/weather.py`

This is the core extraction module. It follows the patterns from `extraction/nyc.py` and `extraction/london.py`: module-level constants, function-based API, idempotency via S3 existence checks, local temp file + upload pattern.

```python
"""
Weather Data Extraction Module

Fetches hourly weather data from Open-Meteo API for NYC and London,
converts to Parquet, and uploads to S3.

Supports two modes:
- Backfill: fetch full historical data from start_year to present, chunked by year
- Incremental: fetch the last N days of data (for monthly pipeline runs)
"""

from dotenv import load_dotenv
load_dotenv()

import os
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.exceptions import RequestException

from extraction.utils import upload_to_s3, file_exists_in_s3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WEATHER_PARQUET_PREFIX = "extracted_weather_parquet"
LOCAL_TMP_DIR = "/tmp/extracted_weather_parquet/"

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# Open-Meteo API endpoints
HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"

# City coordinates
CITY_CONFIGS: Dict[str, Dict] = {
    "nyc": {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "timezone": "America/New_York",
        "start_year": 2013,  # CitiBike launched July 2013
    },
    "london": {
        "latitude": 51.5074,
        "longitude": -0.1278,
        "timezone": "Europe/London",
        "start_year": 2015,  # Santander Cycles data from ~2015 in our pipeline
    },
}

# Hourly variables to request from Open-Meteo
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_gusts_10m",
]

# Rate limiting: seconds to sleep between API requests
REQUEST_DELAY_SECONDS = 0.5


# ---------------------------------------------------------------------------
# API Fetching
# ---------------------------------------------------------------------------

def fetch_historical_weather(
    city: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Fetch hourly historical weather data from Open-Meteo Archive API.

    Args:
        city: City key ("nyc" or "london")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with columns: [time, temperature_2m, relative_humidity_2m,
        apparent_temperature, precipitation, rain, snowfall, snow_depth,
        weather_code, cloud_cover, wind_speed_10m, wind_gusts_10m, city]

    Raises:
        ValueError: If city is not in CITY_CONFIGS
        RequestException: If the API request fails
    """
    if city not in CITY_CONFIGS:
        raise ValueError(f"Unknown city: {city}. Must be one of {list(CITY_CONFIGS.keys())}")

    config = CITY_CONFIGS[city]

    params = {
        "latitude": config["latitude"],
        "longitude": config["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": config["timezone"],
    }

    logger.info(f"Fetching weather for {city}: {start_date} to {end_date}")

    response = requests.get(HISTORICAL_API_URL, params=params, timeout=120)
    response.raise_for_status()

    data = response.json()

    if "hourly" not in data:
        logger.warning(f"No hourly data returned for {city} ({start_date} to {end_date})")
        return pd.DataFrame()

    # Build DataFrame from the hourly dict
    hourly = data["hourly"]
    df = pd.DataFrame(hourly)

    # Add city column
    df["city"] = city

    # Rename 'time' column to 'timestamp' for clarity
    df = df.rename(columns={"time": "timestamp"})

    # Parse timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    logger.info(f"Fetched {len(df)} hourly records for {city} ({start_date} to {end_date})")

    return df


def fetch_forecast_weather(city: str, past_days: int = 5) -> pd.DataFrame:
    """
    Fetch recent + forecast weather data from Open-Meteo Forecast API.

    Useful for incremental updates covering the last few days plus forecast.

    Args:
        city: City key ("nyc" or "london")
        past_days: Number of past days to include (0-92)

    Returns:
        DataFrame with same schema as fetch_historical_weather
    """
    if city not in CITY_CONFIGS:
        raise ValueError(f"Unknown city: {city}. Must be one of {list(CITY_CONFIGS.keys())}")

    config = CITY_CONFIGS[city]

    params = {
        "latitude": config["latitude"],
        "longitude": config["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": config["timezone"],
        "past_days": past_days,
        "forecast_days": 16,
    }

    logger.info(f"Fetching forecast weather for {city} (past {past_days} days + 16-day forecast)")

    response = requests.get(FORECAST_API_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()

    if "hourly" not in data:
        logger.warning(f"No hourly data returned for {city} forecast")
        return pd.DataFrame()

    hourly = data["hourly"]
    df = pd.DataFrame(hourly)
    df["city"] = city
    df = df.rename(columns={"time": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    logger.info(f"Fetched {len(df)} hourly forecast records for {city}")

    return df


# ---------------------------------------------------------------------------
# S3 Upload (Parquet)
# ---------------------------------------------------------------------------

def _write_and_upload_parquet(df: pd.DataFrame, city: str, label: str) -> bool:
    """
    Write a DataFrame to a local Parquet file, then upload to S3.

    Args:
        df: DataFrame to write
        city: City key ("nyc" or "london")
        label: File label (e.g., "2023" or "forecast_2024-01-15")

    Returns:
        True if uploaded, False if already exists or empty DataFrame
    """
    if df.empty:
        logger.info(f"Empty DataFrame for {city}/{label}, skipping")
        return False

    filename = f"weather_{city}_{label}.parquet"
    s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/{filename}"

    # Idempotency: skip if already uploaded
    if file_exists_in_s3(s3_key):
        logger.info(f"Weather file already exists in S3: {s3_key}")
        return False

    local_path = os.path.join(LOCAL_TMP_DIR, filename)

    try:
        # Write Parquet with snappy compression
        df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)

        # Upload to S3
        upload_to_s3(local_path, s3_key)
        logger.info(f"Uploaded weather data to S3: {s3_key} ({len(df)} rows)")
        return True

    except (OSError, RequestException) as e:
        logger.error(f"Failed to write/upload {s3_key}: {e}")
        return False
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


# ---------------------------------------------------------------------------
# Orchestration Functions
# ---------------------------------------------------------------------------

def backfill_city(city: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> Dict[str, bool]:
    """
    Backfill historical weather data for a city, chunked by year.

    Each year is saved as a separate Parquet file for idempotent re-runs.

    Args:
        city: City key ("nyc" or "london")
        start_year: Override start year (default: from CITY_CONFIGS)
        end_year: Override end year (default: current year)

    Returns:
        Dict mapping year labels to upload success status
    """
    config = CITY_CONFIGS[city]
    start = start_year or config["start_year"]
    end = end_year or datetime.now().year

    results = {}

    for year in range(start, end + 1):
        year_start = f"{year}-01-01"
        # For the current year, use yesterday as end date (archive API has ~5 day lag)
        if year == datetime.now().year:
            year_end = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        else:
            year_end = f"{year}-12-31"

        label = str(year)
        s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/weather_{city}_{label}.parquet"

        # Idempotency check before even making the API call
        if file_exists_in_s3(s3_key):
            logger.info(f"Year {year} already exists for {city}, skipping API call")
            results[label] = False
            continue

        try:
            df = fetch_historical_weather(city, year_start, year_end)
            uploaded = _write_and_upload_parquet(df, city, label)
            results[label] = uploaded
        except (RequestException, ValueError) as e:
            logger.error(f"Failed to fetch weather for {city}/{year}: {e}")
            results[label] = False

        # Rate limiting
        time.sleep(REQUEST_DELAY_SECONDS)

    return results


def incremental_update(city: str, days_back: int = 35) -> bool:
    """
    Fetch recent weather data for incremental pipeline updates.

    Uses the Forecast API with past_days to get recent actual + forecast data.
    Overwrites the 'incremental' file each run (NOT idempotent by design --
    we want fresh data each monthly run).

    Args:
        city: City key ("nyc" or "london")
        days_back: Number of past days to include (default 35 for monthly overlap)

    Returns:
        True if successful, False otherwise
    """
    try:
        df = fetch_forecast_weather(city, past_days=min(days_back, 92))

        if df.empty:
            logger.warning(f"No forecast data returned for {city}")
            return False

        label = f"incremental_{datetime.now().strftime('%Y-%m-%d')}"

        filename = f"weather_{city}_{label}.parquet"
        s3_key = f"{WEATHER_PARQUET_PREFIX}/{city}/{filename}"
        local_path = os.path.join(LOCAL_TMP_DIR, filename)

        try:
            df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
            upload_to_s3(local_path, s3_key)
            logger.info(f"Uploaded incremental weather to S3: {s3_key} ({len(df)} rows)")
            return True
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    except (RequestException, ValueError, OSError) as e:
        logger.error(f"Incremental weather update failed for {city}: {e}")
        return False


def backfill_all(start_year: Optional[int] = None, end_year: Optional[int] = None) -> Dict[str, Dict]:
    """
    Backfill historical weather data for ALL cities.

    Args:
        start_year: Override start year for all cities
        end_year: Override end year for all cities

    Returns:
        Dict mapping city names to their per-year results
    """
    logger.info("Starting weather backfill for all cities...")
    all_results = {}

    for city in CITY_CONFIGS:
        logger.info(f"\n{'='*50}")
        logger.info(f"Backfilling weather for {city.upper()}")
        logger.info(f"{'='*50}")

        results = backfill_city(city, start_year=start_year, end_year=end_year)
        all_results[city] = results

        uploaded = sum(1 for v in results.values() if v)
        skipped = sum(1 for v in results.values() if not v)
        logger.info(f"Weather backfill for {city}: {uploaded} uploaded, {skipped} skipped")

    return all_results


def incremental_update_all(days_back: int = 35) -> Dict[str, bool]:
    """
    Run incremental weather update for ALL cities.

    Args:
        days_back: Number of past days to include

    Returns:
        Dict mapping city names to success status
    """
    logger.info("Starting incremental weather update for all cities...")
    results = {}

    for city in CITY_CONFIGS:
        success = incremental_update(city, days_back=days_back)
        results[city] = success
        time.sleep(REQUEST_DELAY_SECONDS)

    return results


# ---------------------------------------------------------------------------
# Entry point for standalone execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Weather data extraction")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental",
                        help="Extraction mode: backfill (full history) or incremental (recent)")
    parser.add_argument("--city", choices=["nyc", "london", "all"], default="all",
                        help="City to extract weather for")
    parser.add_argument("--start-year", type=int, default=None,
                        help="Start year for backfill mode")
    parser.add_argument("--end-year", type=int, default=None,
                        help="End year for backfill mode")
    parser.add_argument("--days-back", type=int, default=35,
                        help="Days of history for incremental mode")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if args.mode == "backfill":
        if args.city == "all":
            backfill_all(start_year=args.start_year, end_year=args.end_year)
        else:
            backfill_city(args.city, start_year=args.start_year, end_year=args.end_year)
    else:
        if args.city == "all":
            incremental_update_all(days_back=args.days_back)
        else:
            incremental_update(args.city, days_back=args.days_back)
```

**Key design decisions:**
- Chunked by year for backfill (one Parquet file per city per year) to keep S3 files manageable and enable idempotent re-runs.
- Uses `file_exists_in_s3()` from `extraction/utils.py` for idempotency, matching the pattern in `extraction/nyc.py` (line 71) and `extraction/london.py` (line 75).
- **Incremental mode uses date-stamped filenames** so each monthly run produces a new file. This is intentional — dbt's incremental logic uses `where source_file not in (select distinct source_file from {{ this }})`, so a new filename triggers reprocessing. The `unique_key='weather_record_id'` in the staging model handles dedup for overlapping data. Accumulation is negligible (~840 rows per file, ~12 files/year/city).
- Rate limiting with `REQUEST_DELAY_SECONDS = 0.5` between requests.
- Local temp file + upload + cleanup pattern matches `extraction/nyc.py` (lines 76-98).

---

### 3.2 NEW FILE: `/Users/chris/Projects/city-cycles/data_models/weather.py`

This follows the pattern from `data_models/nyc_bike.py` and `data_models/london_bike.py`: a `@dataclass` extending `BaseDataRecord` (renamed from `BaseBikeShareRecord` in this phase) with `_required_columns`, `staging_table`, `s3_prefix`, and a `to_dataframe` classmethod.

Weather data is structurally different from bike ride records, but the base class is a generic schema-validation mixin. Its `validate_schema` method works purely on column names. The `to_dataframe` method standardizes column names and types.

```python
"""
Weather Data Model

Data model for hourly weather records from Open-Meteo.
Follows the same pattern as bike share record models.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import pandas as pd
from data_models.base import BaseDataRecord


@dataclass
class HourlyWeatherRecord(BaseDataRecord):
    """Model for hourly weather observations from Open-Meteo API."""

    timestamp: datetime
    city: str
    temperature_2m: float
    relative_humidity_2m: float
    apparent_temperature: float
    precipitation: float
    rain: float
    snowfall: float
    snow_depth: Optional[float]
    weather_code: Optional[int]
    cloud_cover: Optional[float]
    wind_speed_10m: float
    wind_gusts_10m: Optional[float]
    source_file: str

    staging_table = "raw_weather_hourly"
    s3_prefix = "extracted_weather_parquet/"

    # Required columns in the raw data (as output by extraction/weather.py)
    _required_columns = [
        "timestamp",
        "city",
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "weather_code",
        "cloud_cover",
        "wind_speed_10m",
        "wind_gusts_10m",
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Transform raw weather DataFrame into standardized model format.

        No column renames needed -- extraction/weather.py already outputs
        the correct column names. We just add source_file and enforce types.
        """
        df["source_file"] = source_file

        # Ensure timestamp is datetime
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Ensure numeric columns are correct types
        float_cols = [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature",
            "precipitation", "rain", "snowfall", "snow_depth",
            "cloud_cover", "wind_speed_10m", "wind_gusts_10m",
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "weather_code" in df.columns:
            df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce")

        return df[list(cls.__dataclass_fields__.keys())]
```

**Notes:**
- `snow_depth`, `weather_code`, `cloud_cover`, `wind_gusts_10m` are `Optional` because Open-Meteo returns `null` for these in some time periods.
- The `_required_columns` list does not include `snow_depth` and `wind_gusts_10m` because they may legitimately be absent from some API responses. It includes only the columns that should always be present.

---

### 3.3 NEW FILE: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/staging/stg_weather_hourly.sql`

This follows the patterns from `stg_nyc_modern.sql` and `stg_london_modern.sql`: incremental materialization, `source_file`-based incremental logic, type casting, and derived fields.

```sql
{{ config(
    materialized='incremental',
    unique_key='weather_record_id',
    indexes=[
        {'columns': ['timestamp']},
        {'columns': ['city']},
        {'columns': ['date']},
        {'columns': ['weather_record_id'], 'unique': true}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_weather_hourly') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

cleaned as (
    select
        -- Generate a unique record ID from city + timestamp
        city || '_' || strftime(timestamp::timestamp, '%Y%m%d%H') as weather_record_id,

        -- Core fields
        timestamp::timestamp as timestamp,
        city,

        -- Temperature fields (Celsius)
        temperature_2m::double precision as temperature_celsius,
        apparent_temperature::double precision as apparent_temperature_celsius,

        -- Humidity
        relative_humidity_2m::double precision as relative_humidity_pct,

        -- Precipitation (mm)
        precipitation::double precision as precipitation_mm,
        rain::double precision as rain_mm,
        snowfall::double precision as snowfall_cm,
        snow_depth::double precision as snow_depth_m,

        -- WMO weather code
        weather_code::integer as weather_code,

        -- Cloud & wind
        cloud_cover::double precision as cloud_cover_pct,
        wind_speed_10m::double precision as wind_speed_kmh,
        wind_gusts_10m::double precision as wind_gusts_kmh,

        -- Derived: human-readable weather condition from WMO code
        CASE
            WHEN weather_code IN (0) THEN 'clear'
            WHEN weather_code IN (1, 2, 3) THEN 'partly_cloudy'
            WHEN weather_code IN (45, 48) THEN 'fog'
            WHEN weather_code IN (51, 53, 55) THEN 'drizzle'
            WHEN weather_code IN (56, 57) THEN 'freezing_drizzle'
            WHEN weather_code IN (61, 63, 65) THEN 'rain'
            WHEN weather_code IN (66, 67) THEN 'freezing_rain'
            WHEN weather_code IN (71, 73, 75) THEN 'snow'
            WHEN weather_code IN (77) THEN 'snow_grains'
            WHEN weather_code IN (80, 81, 82) THEN 'rain_showers'
            WHEN weather_code IN (85, 86) THEN 'snow_showers'
            WHEN weather_code IN (95) THEN 'thunderstorm'
            WHEN weather_code IN (96, 99) THEN 'thunderstorm_hail'
            ELSE 'unknown'
        END as weather_condition,

        -- Derived: is_precipitation flag
        CASE
            WHEN precipitation > 0 OR rain > 0 OR snowfall > 0 THEN true
            ELSE false
        END as is_precipitation,

        -- Derived: precipitation intensity category
        CASE
            WHEN precipitation = 0 THEN 'none'
            WHEN precipitation < 2.5 THEN 'light'
            WHEN precipitation < 7.5 THEN 'moderate'
            WHEN precipitation < 50 THEN 'heavy'
            ELSE 'extreme'
        END as precipitation_intensity,

        -- Derived: temperature band
        CASE
            WHEN temperature_2m < 0 THEN 'freezing'
            WHEN temperature_2m < 10 THEN 'cold'
            WHEN temperature_2m < 20 THEN 'mild'
            WHEN temperature_2m < 30 THEN 'warm'
            ELSE 'hot'
        END as temperature_band,

        -- Derived: wind category (Beaufort-inspired, km/h)
        CASE
            WHEN wind_speed_10m < 12 THEN 'calm'
            WHEN wind_speed_10m < 30 THEN 'light'
            WHEN wind_speed_10m < 50 THEN 'moderate'
            WHEN wind_speed_10m < 75 THEN 'strong'
            ELSE 'severe'
        END as wind_category,

        -- Date-derived fields (matching bike staging model patterns)
        date_trunc('day', timestamp::timestamp) as date,
        date_trunc('hour', timestamp::timestamp) as hour,
        extract(month from timestamp::timestamp) as month,
        extract(year from timestamp::timestamp) as year,
        {{ day_type('timestamp') }} AS day_type,
        extract(isodow from timestamp::timestamp) - 1 as day_of_week,
        extract(hour from timestamp::timestamp) as hour_of_day,

        -- Metadata
        source_file,
        current_timestamp as dbt_updated_at

    from source
    -- Filter out potential null timestamps
    where timestamp is not null
)

select * from cleaned
```

**Design notes:**
- `weather_record_id` is `city_YYYYMMDDHH` -- guaranteed unique per city per hour.
- WMO weather codes are mapped to human-readable categories per the WMO standard (codes 0-99).
- The precipitation intensity thresholds follow meteorological conventions (mm/hr).
- `date`, `month`, `year`, `day_type`, `day_of_week`, `hour_of_day` match the bike staging models exactly -- this enables easy joins in future intermediate models.
- Uses the existing `day_type` macro from `/Users/chris/Projects/city-cycles/dbt_city_cycles/macros/day_type.sql`.

---

### 3.4 NEW FILE: `/Users/chris/Projects/city-cycles/tests/test_weather_extraction.py`

Tests for the weather extraction module. Follows the patterns in `tests/test_extraction.py`.

```python
"""
Tests for the weather extraction module.

Tests extraction/weather.py functions.

Uses mocked HTTP requests and S3 operations -- no real API calls or AWS access.
Because extraction.utils has module-level side effects, imports are done
inside test functions using the same pattern as test_extraction.py.
"""

import pytest
import os
import sys
import json
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _import_weather(mock_boto_client):
    """
    Import extraction.weather with mocked environment and boto3.

    Args:
        mock_boto_client: The MagicMock to return from boto3.client()

    Returns:
        The extraction.weather module (freshly imported)
    """
    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("extraction"):
            del sys.modules[mod_name]

    with patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}), \
         patch("boto3.client", return_value=mock_boto_client):
        import extraction.weather as weather_mod
        return weather_mod


def _make_api_response(num_hours: int = 24, city: str = "nyc") -> dict:
    """Create a mock Open-Meteo API response JSON."""
    base_time = "2023-06-15T00:00"
    times = [f"2023-06-15T{h:02d}:00" for h in range(num_hours)]

    return {
        "latitude": 40.7 if city == "nyc" else 51.5,
        "longitude": -74.0 if city == "nyc" else -0.13,
        "timezone": "America/New_York" if city == "nyc" else "Europe/London",
        "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
        "hourly": {
            "time": times,
            "temperature_2m": [20.0 + i * 0.5 for i in range(num_hours)],
            "relative_humidity_2m": [65.0] * num_hours,
            "apparent_temperature": [19.0 + i * 0.5 for i in range(num_hours)],
            "precipitation": [0.0] * num_hours,
            "rain": [0.0] * num_hours,
            "snowfall": [0.0] * num_hours,
            "snow_depth": [0.0] * num_hours,
            "weather_code": [0] * num_hours,
            "cloud_cover": [25.0] * num_hours,
            "wind_speed_10m": [10.0] * num_hours,
            "wind_gusts_10m": [15.0] * num_hours,
        },
    }


# ---------------------------------------------------------------------------
# Tests for fetch_historical_weather
# ---------------------------------------------------------------------------

class TestFetchHistoricalWeather:
    """Tests for extraction/weather.py fetch_historical_weather()."""

    def test_fetch_returns_dataframe_with_correct_columns(self):
        """fetch_historical_weather() should return a DataFrame with expected columns."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(24, "nyc")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            df = weather.fetch_historical_weather("nyc", "2023-06-15", "2023-06-15")

        assert len(df) == 24
        assert "timestamp" in df.columns
        assert "city" in df.columns
        assert "temperature_2m" in df.columns
        assert "wind_speed_10m" in df.columns
        assert df["city"].iloc[0] == "nyc"

    def test_fetch_raises_on_invalid_city(self):
        """fetch_historical_weather() should raise ValueError for unknown city."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        with pytest.raises(ValueError, match="Unknown city"):
            weather.fetch_historical_weather("paris", "2023-01-01", "2023-01-01")

    def test_fetch_returns_empty_df_when_no_hourly_data(self):
        """fetch_historical_weather() should return empty DataFrame when API returns no hourly key."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = {"latitude": 40.7, "longitude": -74.0}
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            df = weather.fetch_historical_weather("nyc", "2023-06-15", "2023-06-15")

        assert df.empty

    def test_fetch_sends_correct_api_parameters(self):
        """fetch_historical_weather() should send correct parameters to the API."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(1, "london")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response) as mock_get:
            weather.fetch_historical_weather("london", "2023-01-01", "2023-01-31")

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["latitude"] == 51.5074
        assert params["longitude"] == -0.1278
        assert params["timezone"] == "Europe/London"
        assert params["start_date"] == "2023-01-01"
        assert params["end_date"] == "2023-01-31"


# ---------------------------------------------------------------------------
# Tests for backfill_city
# ---------------------------------------------------------------------------

class TestBackfillCity:
    """Tests for extraction/weather.py backfill_city()."""

    def test_backfill_skips_existing_years(self):
        """backfill_city() should skip years that already exist in S3."""
        mock_s3 = MagicMock()
        # head_object succeeds = file exists
        mock_s3.head_object.return_value = {"ContentLength": 1234}
        mock_s3.exceptions.ClientError = Exception

        weather = _import_weather(mock_s3)

        with patch("extraction.weather.requests.get") as mock_get:
            results = weather.backfill_city("nyc", start_year=2023, end_year=2023)

        # Should NOT make any API calls
        mock_get.assert_not_called()
        assert results["2023"] is False

    def test_backfill_fetches_and_uploads_missing_years(self):
        """backfill_city() should fetch and upload years that do not exist in S3."""
        mock_s3 = MagicMock()

        from botocore.exceptions import ClientError
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(24, "nyc")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response), \
             patch("extraction.weather.time.sleep"):  # Skip rate limiting in tests
            results = weather.backfill_city("nyc", start_year=2023, end_year=2023)

        assert results["2023"] is True
        mock_s3.upload_file.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for incremental_update
# ---------------------------------------------------------------------------

class TestIncrementalUpdate:
    """Tests for extraction/weather.py incremental_update()."""

    def test_incremental_returns_true_on_success(self):
        """incremental_update() should return True when data is fetched and uploaded."""
        mock_s3 = MagicMock()
        mock_s3.exceptions.ClientError = Exception

        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = _make_api_response(48, "london")
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            result = weather.incremental_update("london", days_back=5)

        assert result is True
        mock_s3.upload_file.assert_called_once()

    def test_incremental_returns_false_on_empty_response(self):
        """incremental_update() should return False when API returns no data."""
        mock_s3 = MagicMock()
        weather = _import_weather(mock_s3)

        mock_response = MagicMock()
        mock_response.json.return_value = {"latitude": 40.7}
        mock_response.raise_for_status = MagicMock()

        with patch("extraction.weather.requests.get", return_value=mock_response):
            result = weather.incremental_update("nyc")

        assert result is False


# ---------------------------------------------------------------------------
# Tests for HourlyWeatherRecord data model
# ---------------------------------------------------------------------------

class TestHourlyWeatherRecord:
    """Tests for data_models/weather.py HourlyWeatherRecord."""

    def test_validate_schema_passes_with_all_columns(self):
        """HourlyWeatherRecord.validate_schema() should pass with all required columns."""
        from data_models.weather import HourlyWeatherRecord

        df = pd.DataFrame({
            "timestamp": ["2023-06-15T12:00"],
            "city": ["nyc"],
            "temperature_2m": [25.0],
            "relative_humidity_2m": [65.0],
            "apparent_temperature": [24.0],
            "precipitation": [0.0],
            "rain": [0.0],
            "snowfall": [0.0],
            "weather_code": [0],
            "cloud_cover": [25.0],
            "wind_speed_10m": [10.0],
            "wind_gusts_10m": [15.0],
        })

        assert HourlyWeatherRecord.validate_schema(df) is True

    def test_validate_schema_fails_with_missing_columns(self):
        """HourlyWeatherRecord.validate_schema() should fail when required columns are missing."""
        from data_models.weather import HourlyWeatherRecord

        df = pd.DataFrame({
            "timestamp": ["2023-06-15T12:00"],
            "city": ["nyc"],
            # Missing most required columns
        })

        assert HourlyWeatherRecord.validate_schema(df) is False

    def test_to_dataframe_adds_source_file(self):
        """HourlyWeatherRecord.to_dataframe() should add source_file column."""
        from data_models.weather import HourlyWeatherRecord

        df = pd.DataFrame({
            "timestamp": ["2023-06-15T12:00"],
            "city": ["nyc"],
            "temperature_2m": [25.0],
            "relative_humidity_2m": [65.0],
            "apparent_temperature": [24.0],
            "precipitation": [0.0],
            "rain": [0.0],
            "snowfall": [0.0],
            "snow_depth": [0.0],
            "weather_code": [0],
            "cloud_cover": [25.0],
            "wind_speed_10m": [10.0],
            "wind_gusts_10m": [15.0],
        })

        result = HourlyWeatherRecord.to_dataframe(df, "weather_nyc_2023.parquet")
        assert "source_file" in result.columns
        assert result["source_file"].iloc[0] == "weather_nyc_2023.parquet"
```

---

## 4. Modifications to Existing Files

### 4.0 REFACTOR: Rename `BaseBikeShareRecord` → `BaseDataRecord`

The base class in `data_models/base.py` is a generic schema-validation mixin with no bike-specific logic. Renaming it to `BaseDataRecord` makes the inheritance hierarchy accurate for weather (and future non-bike data types).

**Files to update (mechanical find-and-replace of `BaseBikeShareRecord` → `BaseDataRecord`):**

| File | Occurrences |
|------|-------------|
| `data_models/base.py` | 4 (class def, type hint, 2x registry refs) |
| `data_models/nyc_bike.py` | 3 (import, 2 class defs) |
| `data_models/london_bike.py` | 3 (import, 2 class defs) |
| `extracted_file_manager/manager.py` | 2 (import, registry access) |
| `tests/test_data_models_integration.py` | 8 (imports, registry access) |
| `data_models/README.md` | 3 (class name references) |

**Do NOT update archived documentation** in `documentation/archive/` — those are historical records.

---

### 4.1 MODIFY: `/Users/chris/Projects/city-cycles/extraction/__init__.py`

**Before:**
```python
"""
Extraction Package

Handles data extraction from web sources for bike share data.
"""

from .nyc import download_all_zips, list_nyc_citibike_files, download_and_store_zip
from .london import download_all_csvs, list_london_csv_files, download_and_store_csv

__all__ = [
    # NYC functions
    'download_all_zips',
    'list_nyc_citibike_files', 
    'download_and_store_zip',
    # London functions
    'download_all_csvs',
    'list_london_csv_files',
    'download_and_store_csv'
]
```

**After:**
```python
"""
Extraction Package

Handles data extraction from web sources for bike share and weather data.
"""

from .nyc import download_all_zips, list_nyc_citibike_files, download_and_store_zip
from .london import download_all_csvs, list_london_csv_files, download_and_store_csv
from .weather import (
    backfill_all as weather_backfill_all,
    incremental_update_all as weather_incremental_update_all,
    backfill_city as weather_backfill_city,
    incremental_update as weather_incremental_update,
)

__all__ = [
    # NYC functions
    'download_all_zips',
    'list_nyc_citibike_files', 
    'download_and_store_zip',
    # London functions
    'download_all_csvs',
    'list_london_csv_files',
    'download_and_store_csv',
    # Weather functions
    'weather_backfill_all',
    'weather_incremental_update_all',
    'weather_backfill_city',
    'weather_incremental_update',
]
```

---

### 4.2 MODIFY: `/Users/chris/Projects/city-cycles/data_models/__init__.py`

**Before:**
```python
# Import models for easier access
from .london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from .nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord

__all__ = [
    'NYCModernBikeShareRecord',
    'NYCLegacyBikeShareRecord',
    'LondonModernBikeShareRecord',
    'LondonLegacyBikeShareRecord'
]
```

**After:**
```python
# Import models for easier access
from .london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from .nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord
from .weather import HourlyWeatherRecord

__all__ = [
    'NYCModernBikeShareRecord',
    'NYCLegacyBikeShareRecord',
    'LondonModernBikeShareRecord',
    'LondonLegacyBikeShareRecord',
    'HourlyWeatherRecord',
]
```

---

### 4.3 MODIFY: `/Users/chris/Projects/city-cycles/data_models/registry.py`

**Before:**
```python
from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord

MODEL_REGISTRY = [
    LondonLegacyBikeShareRecord,
    LondonModernBikeShareRecord,
    NYCLegacyBikeShareRecord,
    NYCModernBikeShareRecord,
]
```

**After:**
```python
from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord
from data_models.nyc_bike import NYCLegacyBikeShareRecord, NYCModernBikeShareRecord
from data_models.weather import HourlyWeatherRecord

MODEL_REGISTRY = [
    LondonLegacyBikeShareRecord,
    LondonModernBikeShareRecord,
    NYCLegacyBikeShareRecord,
    NYCModernBikeShareRecord,
    HourlyWeatherRecord,
]
```

---

### 4.4 MODIFY: `/Users/chris/Projects/city-cycles/db_duckdb/config/duckdb_config.py`

Add the new weather table schema, S3 URI, and validation query. Insert new entries into the existing dictionaries.

**Add to `S3_URIS` dict (after line 24):**
```python
    'raw_weather_hourly': f's3://{S3_BUCKET}/extracted_weather_parquet/*/*.parquet',
```

Note: The glob `*/*.parquet` captures both `nyc/*.parquet` and `london/*.parquet` subdirectories.

**Add to `TABLE_SCHEMAS` dict (after the `raw_london_modern` entry, after line 99):**
```python
    'raw_weather_hourly': """
        CREATE TABLE raw_weather_hourly (
            timestamp TIMESTAMP,
            city VARCHAR,
            temperature_2m DOUBLE,
            relative_humidity_2m DOUBLE,
            apparent_temperature DOUBLE,
            precipitation DOUBLE,
            rain DOUBLE,
            snowfall DOUBLE,
            snow_depth DOUBLE,
            weather_code INTEGER,
            cloud_cover DOUBLE,
            wind_speed_10m DOUBLE,
            wind_gusts_10m DOUBLE,
            source_file VARCHAR
        )
    """,
```

**Add to `VALIDATION_QUERIES` dict (after the `raw_london_modern` entry):**
```python
    'raw_weather_hourly': """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            COUNT(DISTINCT city) as unique_cities,
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp,
            COUNT(DISTINCT date_trunc('day', timestamp)) as unique_days
        FROM raw_weather_hourly
    """,
```

---

### 4.5 MODIFY: `/Users/chris/Projects/city-cycles/db_duckdb/operations.py`

Add weather quality check config to `TABLE_QUALITY_CONFIG` dict (after line 41):

```python
    'raw_weather_hourly': {
        'null_check_columns': ['timestamp', 'city', 'temperature_2m', 'wind_speed_10m'],
        'duplicate_key': None,  # Duplicates are expected (city+timestamp combo is the key)
        'date_columns': ('timestamp', 'timestamp'),
    },
```

---

### 4.6 MODIFY: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/staging/sources.yml`

Add the weather raw source table. Insert after the `raw_london_modern` table entry (after line 114).

**Add:**
```yaml
      - name: raw_weather_hourly
        description: >
          Raw hourly weather observations from Open-Meteo API for NYC and London.
          Contains temperature, precipitation, wind, cloud cover, and WMO weather codes.
        loaded_at_field: "timestamp::timestamp"
        columns:
          - name: timestamp
            description: Observation timestamp (local time for each city)
            tests:
              - not_null
          - name: city
            description: City identifier (nyc or london)
            tests:
              - not_null
              - accepted_values:
                  values: ['nyc', 'london']
          - name: temperature_2m
            description: Air temperature at 2 meters above ground (Celsius)
          - name: precipitation
            description: Total precipitation (mm)
          - name: weather_code
            description: WMO weather interpretation code
          - name: wind_speed_10m
            description: Wind speed at 10 meters above ground (km/h)
          - name: source_file
            description: Source Parquet file name for lineage tracking
```

---

### 4.7 NEW FILE: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/staging/stg_weather_hourly.sql`

(Full content already provided in section 3.3 above.)

---

### 4.8 MODIFY: `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/staging/schema.yml`

Add the stg_weather_hourly model definition. Append after the `stg_london_modern` model (after line 259).

**Add:**
```yaml
  - name: stg_weather_hourly
    description: >
      Staged hourly weather observations for NYC and London from Open-Meteo.
      Includes raw measurements plus derived fields: weather_condition (from WMO code),
      is_precipitation, precipitation_intensity, temperature_band, and wind_category.
    columns:
      - name: weather_record_id
        description: Unique identifier for each weather observation (city_YYYYMMDDHH)
        tests:
          - unique
          - not_null
      - name: timestamp
        description: Observation timestamp
        tests:
          - not_null
      - name: city
        description: City identifier (nyc or london)
        tests:
          - not_null
          - accepted_values:
              values: ['nyc', 'london']
      - name: temperature_celsius
        description: Air temperature at 2m above ground in Celsius
      - name: apparent_temperature_celsius
        description: Feels-like temperature in Celsius
      - name: relative_humidity_pct
        description: Relative humidity as percentage
      - name: precipitation_mm
        description: Total precipitation in millimeters
      - name: rain_mm
        description: Rainfall in millimeters
      - name: snowfall_cm
        description: Snowfall in centimeters
      - name: snow_depth_m
        description: Snow depth in meters
      - name: weather_code
        description: WMO weather interpretation code (0-99)
      - name: cloud_cover_pct
        description: Cloud cover as percentage
      - name: wind_speed_kmh
        description: Wind speed at 10m in km/h
      - name: wind_gusts_kmh
        description: Wind gusts at 10m in km/h
      - name: weather_condition
        description: Human-readable weather condition derived from WMO code
        tests:
          - accepted_values:
              values: ['clear', 'partly_cloudy', 'fog', 'drizzle', 'freezing_drizzle', 'rain', 'freezing_rain', 'snow', 'snow_grains', 'rain_showers', 'snow_showers', 'thunderstorm', 'thunderstorm_hail', 'unknown']
      - name: is_precipitation
        description: Boolean flag indicating whether any precipitation occurred
      - name: precipitation_intensity
        description: "Precipitation intensity category: none, light, moderate, heavy, extreme"
        tests:
          - accepted_values:
              values: ['none', 'light', 'moderate', 'heavy', 'extreme']
      - name: temperature_band
        description: "Temperature category: freezing, cold, mild, warm, hot"
        tests:
          - accepted_values:
              values: ['freezing', 'cold', 'mild', 'warm', 'hot']
      - name: wind_category
        description: "Wind category: calm, light, moderate, strong, severe"
        tests:
          - accepted_values:
              values: ['calm', 'light', 'moderate', 'strong', 'severe']
      - name: date
        description: Observation date (truncated to day)
      - name: hour
        description: Observation hour (truncated to hour)
      - name: month
        description: Month number
      - name: year
        description: Year
      - name: day_type
        description: "weekday or weekend"
        tests:
          - accepted_values:
              values: ['weekday', 'weekend']
      - name: day_of_week
        description: "Day of week (0=Monday, 6=Sunday)"
      - name: hour_of_day
        description: "Hour of day (0-23)"
      - name: source_file
        description: Source file for lineage tracking
      - name: dbt_updated_at
        description: Timestamp of when dbt last processed this row
```

---

### 4.9 MODIFY: `/Users/chris/Projects/city-cycles/orchestrator/main.py`

The orchestrator's `_run_extraction` method (line 118) needs to include weather extraction. The weather extraction should be added as a third sub-step within the existing extraction step, and a new dedicated stage `weather_extraction` should be available for `run_stage`.

**Modify `_run_extraction` method -- add after London extraction (after line 148, before the `self.results['extraction']` line):**

```python
            # Weather extraction
            logger.info("\n-> Extracting weather data from Open-Meteo...")
            try:
                from extraction import weather
                weather.incremental_update_all(days_back=35)
                logger.info("-> Weather extraction completed")
            except Exception as e:
                logger.error(f"-> Weather extraction failed: {e}")
                # Continue -- weather is supplementary data
```

**Modify `run_stage` method (line 352) -- add a new elif branch after the `extraction` branch:**

```python
            elif stage == 'weather_extraction':
                self._run_weather_extraction()
```

**Add new method `_run_weather_extraction` to the class:**

```python
    def _run_weather_extraction(self):
        """
        Run weather data extraction independently.

        Supports both backfill and incremental modes via kwargs.
        """
        _log_step(1, 1, "EXTRACTING WEATHER DATA FROM OPEN-METEO")

        try:
            from extraction import weather

            logger.info("\n-> Running weather incremental update...")
            results = weather.incremental_update_all(days_back=35)

            self.results['weather_extraction'] = {
                'status': 'success',
                'results': results
            }
            logger.info("\n-> Weather extraction phase completed")

        except Exception as e:
            logger.error(f"\n-> Weather extraction phase failed: {e}")
            raise RuntimeError(f"Weather extraction phase failed: {e}")
```

---

### 4.10 MODIFY: `/Users/chris/Projects/city-cycles/orchestrator/cli.py`

Add `weather_extraction` to the valid stage choices.

**Modify line 86 -- update `choices` list:**

**Before:**
```python
        'stage_name',
        choices=['extraction', 'file_management', 'database_load', 'dbt', 'export'],
```

**After:**
```python
        'stage_name',
        choices=['extraction', 'weather_extraction', 'file_management', 'database_load', 'dbt', 'export'],
```

---

### 4.11 MODIFY: `/Users/chris/Projects/city-cycles/dbt_city_cycles/dbt_project.yml`

No changes needed. The staging model will inherit `+materialized: view` from the existing staging configuration. However, since `stg_weather_hourly.sql` uses `{{ config(materialized='incremental', ...) }}` at the top, it overrides the directory-level setting. This is the same pattern as `stg_nyc_modern.sql` -- the individual model config takes precedence.

---

### 4.12 MODIFY: `/Users/chris/Projects/city-cycles/tests/conftest.py`

Add a sample weather DataFrame fixture.

**Add after the `sample_london_modern_df` fixture (after line 173):**

```python
@pytest.fixture
def sample_weather_df():
    """
    Sample DataFrame matching the hourly weather schema.

    Contains 2 rows of weather data as output by extraction/weather.py.
    """
    return pd.DataFrame({
        "timestamp": ["2023-06-15T12:00", "2023-06-15T13:00"],
        "city": ["nyc", "nyc"],
        "temperature_2m": [25.0, 26.5],
        "relative_humidity_2m": [65.0, 60.0],
        "apparent_temperature": [24.0, 25.5],
        "precipitation": [0.0, 2.5],
        "rain": [0.0, 2.5],
        "snowfall": [0.0, 0.0],
        "snow_depth": [0.0, 0.0],
        "weather_code": [0, 61],
        "cloud_cover": [25.0, 80.0],
        "wind_speed_10m": [10.0, 20.0],
        "wind_gusts_10m": [15.0, 30.0],
        "source_file": ["weather_nyc_2023.parquet", "weather_nyc_2023.parquet"],
    })
```

---

## 5. dbt Models -- Complete SQL Reference

### WMO Weather Code Reference Table
The WMO codes used in `stg_weather_hourly.sql` follow this standard:

| Code | Meaning |
|------|---------|
| 0 | Clear sky |
| 1, 2, 3 | Mainly clear, partly cloudy, overcast |
| 45, 48 | Fog, depositing rime fog |
| 51, 53, 55 | Drizzle: light, moderate, dense |
| 56, 57 | Freezing drizzle: light, dense |
| 61, 63, 65 | Rain: slight, moderate, heavy |
| 66, 67 | Freezing rain: light, heavy |
| 71, 73, 75 | Snow fall: slight, moderate, heavy |
| 77 | Snow grains |
| 80, 81, 82 | Rain showers: slight, moderate, violent |
| 85, 86 | Snow showers: slight, heavy |
| 95 | Thunderstorm: slight or moderate |
| 96, 99 | Thunderstorm with slight/heavy hail |

These are the complete set of codes the Open-Meteo API returns.

---

## 6. Tests Summary

### New test file: `/Users/chris/Projects/city-cycles/tests/test_weather_extraction.py`
(Full content in section 3.4 above)

**Test classes and methods:**

| Class | Method | What it verifies |
|-------|--------|-----------------|
| `TestFetchHistoricalWeather` | `test_fetch_returns_dataframe_with_correct_columns` | API response is parsed into correct DataFrame shape |
| `TestFetchHistoricalWeather` | `test_fetch_raises_on_invalid_city` | ValueError for unknown city |
| `TestFetchHistoricalWeather` | `test_fetch_returns_empty_df_when_no_hourly_data` | Graceful handling of missing `hourly` key |
| `TestFetchHistoricalWeather` | `test_fetch_sends_correct_api_parameters` | Correct coordinates, timezone, dates sent |
| `TestBackfillCity` | `test_backfill_skips_existing_years` | Idempotency -- no API call when S3 file exists |
| `TestBackfillCity` | `test_backfill_fetches_and_uploads_missing_years` | Full fetch + upload for missing years |
| `TestIncrementalUpdate` | `test_incremental_returns_true_on_success` | Happy path for incremental |
| `TestIncrementalUpdate` | `test_incremental_returns_false_on_empty_response` | Graceful handling of empty forecast |
| `TestHourlyWeatherRecord` | `test_validate_schema_passes_with_all_columns` | Schema validation passes |
| `TestHourlyWeatherRecord` | `test_validate_schema_fails_with_missing_columns` | Schema validation fails correctly |
| `TestHourlyWeatherRecord` | `test_to_dataframe_adds_source_file` | `to_dataframe` adds metadata |

**Expected test count impact:** +11 tests (baseline is 83 pass, 3 skip).

---

## 7. Verification Checklist

After implementing, the junior team should verify each item:

### Base Class Rename
- [ ] Verify `BaseBikeShareRecord` does not appear in any source files (excluding archived docs): `grep -r "BaseBikeShareRecord" --include="*.py" .`
- [ ] Import smoke test: `venv/bin/python -c "from data_models.base import BaseDataRecord; print('OK')"`

### Python Tests
- [ ] Run `venv/bin/python -m pytest tests/test_weather_extraction.py -v` -- all 11 tests pass
- [ ] Run `venv/bin/python -m pytest tests/ -v` -- baseline 83 + 11 new = 94 pass (no regressions)
- [ ] Import smoke test: `venv/bin/python -c "from extraction.weather import backfill_all, incremental_update_all; print('OK')"`
- [ ] Import smoke test: `venv/bin/python -c "from data_models.weather import HourlyWeatherRecord; print('OK')"`
- [ ] Import smoke test: `venv/bin/python -c "from data_models.registry import MODEL_REGISTRY; assert len(MODEL_REGISTRY) == 5; print('OK')"`

### DuckDB Config
- [ ] Verify `raw_weather_hourly` appears in `TABLE_SCHEMAS`, `S3_URIS`, and `VALIDATION_QUERIES` in `db_duckdb/config/duckdb_config.py`
- [ ] Run `venv/bin/python -c "from db_duckdb.config.duckdb_config import TABLE_SCHEMAS, S3_URIS, VALIDATION_QUERIES; assert 'raw_weather_hourly' in TABLE_SCHEMAS; assert 'raw_weather_hourly' in S3_URIS; assert 'raw_weather_hourly' in VALIDATION_QUERIES; print('OK')"`

### dbt
- [ ] Run `cd dbt_city_cycles && dbt compile --select stg_weather_hourly` -- compiles without error
- [ ] Run `cd dbt_city_cycles && dbt test --select stg_weather_hourly` -- dbt tests pass (after data is loaded)
- [ ] Verify `stg_weather_hourly` appears in `sources.yml` and `schema.yml`

### Orchestrator
- [ ] Run `venv/bin/python -m orchestrator.cli stage --help` -- `weather_extraction` appears in choices
- [ ] Run `venv/bin/python -m extraction.weather --mode incremental --city nyc` -- fetches and uploads (requires AWS credentials)

### End-to-End (with credentials)
- [ ] Backfill one year: `venv/bin/python -m extraction.weather --mode backfill --city nyc --start-year 2023 --end-year 2023`
- [ ] Verify S3 file exists: `aws s3 ls s3://city-cycles-data-ctr37/extracted_weather_parquet/nyc/`
- [ ] Load into DuckDB: `venv/bin/python -m db_duckdb.cli load --table raw_weather_hourly`
- [ ] Run dbt: `cd dbt_city_cycles && dbt run --select stg_weather_hourly`
- [ ] Query result: `dbt run-operation dbt_city_cycles.print_table --args '{table: stg_weather_hourly}'` OR query via DuckDB directly

---

## 8. What NOT To Do

1. **Do NOT add `openmeteo-sdk` or `openmeteo-requests` to requirements.txt.** The Open-Meteo API is a simple REST API. Plain `requests` (already a dependency at line 65 of requirements.txt) is sufficient. Adding unnecessary dependencies increases surface area and breaks the existing convention.

2. **Do NOT bypass S3 for weather data.** All other data in the pipeline goes through S3. Weather must follow the same path: API -> Parquet -> S3 -> DuckDB -> dbt. Do not write directly to DuckDB from the API.

3. **Do NOT make weather extraction blocking.** If weather API fails, the bike data pipeline must continue. The `_run_extraction` modification uses a try/except that logs but continues, matching the pattern for NYC/London extraction failures (lines 137-148 of `orchestrator/main.py`).

4. **Do NOT hardcode the S3 bucket name.** Use `os.environ.get("S3_BUCKET")` via `extraction/utils.py` functions, not literal bucket names.

5. **Do NOT process weather through `extracted_file_manager`.** That pipeline is for ZIP/CSV processing. Weather goes directly from API JSON to Parquet. The `extracted_file_manager` module should not be modified.

6. **Do NOT create a new DuckDB database or connection class for weather.** Use the existing `DuckDBManager` and `DuckDBOperations` classes. The weather table is just another raw table.

7. **Do NOT skip the `source_file` column.** Every raw table in the project has `source_file` for lineage tracking and incremental processing. The dbt incremental logic depends on it.

8. **Do NOT use Fahrenheit or mph.** Open-Meteo defaults to Celsius and km/h. Keep these defaults for consistency. Conversions can happen at the dashboard layer if needed.

9. **Do NOT chunk API requests by day or month.** Open-Meteo handles year-long ranges fine. Chunking by year gives a good balance of file size (~8,760 rows per city per year) and idempotency granularity.

10. **Do NOT add weather data to the `streamlit_data_manager/parquet_file_manager.py` in this phase.** That file manages mart Parquet files. Weather will be exposed through a mart in a future phase. This phase delivers only the raw + staging pipeline.

11. **Do NOT modify the `dbt_project.yml` materialization settings.** The `stg_weather_hourly.sql` model overrides with its own `{{ config(materialized='incremental') }}` block, which is the established pattern.

12. **Do NOT use `pd.read_json()` to parse the API response.** Use `requests.get().json()` and then construct the DataFrame from the `hourly` dict manually. The Open-Meteo JSON response structure requires this approach.

---

## 9. Implementation Sequence

Execute in this order (dependencies flow top to bottom):

| Step | File(s) | Depends On | Estimated Effort |
|------|---------|------------|-----------------|
| 0 | Rename `BaseBikeShareRecord` → `BaseDataRecord` (6 files) | Nothing | 15 min |
| 1 | `data_models/weather.py` (new) | Step 0 | 30 min |
| 2 | `data_models/__init__.py` (modify) | Step 1 | 5 min |
| 3 | `data_models/registry.py` (modify) | Step 1 | 5 min |
| 4 | `extraction/weather.py` (new) | Step 1 | 90 min |
| 5 | `extraction/__init__.py` (modify) | Step 4 | 5 min |
| 6 | `db_duckdb/config/duckdb_config.py` (modify) | Step 1 | 15 min |
| 7 | `db_duckdb/operations.py` (modify) | Step 6 | 10 min |
| 8 | `dbt_city_cycles/models/staging/sources.yml` (modify) | Step 6 | 10 min |
| 9 | `dbt_city_cycles/models/staging/stg_weather_hourly.sql` (new) | Step 8 | 45 min |
| 10 | `dbt_city_cycles/models/staging/schema.yml` (modify) | Step 9 | 20 min |
| 11 | `orchestrator/main.py` (modify) | Step 4 | 20 min |
| 12 | `orchestrator/cli.py` (modify) | Step 11 | 5 min |
| 13 | `tests/conftest.py` (modify) | Step 1 | 5 min |
| 14 | `tests/test_weather_extraction.py` (new) | Steps 1, 4 | 60 min |
| 15 | `tests/test_data_models_integration.py` (modify) | Step 0 | 5 min |
| 16 | Run full test suite | Steps 0-15 | 10 min |
| 17 | Update `CHANGELOG.md` | Steps 0-15 | 10 min |

**Total estimated effort:** ~6 hours

---

### Critical Files for Implementation
- `/Users/chris/Projects/city-cycles/extraction/weather.py` - Core extraction module to create (most complex new file, API integration + S3 upload + backfill/incremental modes)
- `/Users/chris/Projects/city-cycles/data_models/weather.py` - Data model to create (HourlyWeatherRecord dataclass, schema validation)
- `/Users/chris/Projects/city-cycles/db_duckdb/config/duckdb_config.py` - Configuration to modify (TABLE_SCHEMAS, S3_URIS, VALIDATION_QUERIES for raw_weather_hourly)
- `/Users/chris/Projects/city-cycles/dbt_city_cycles/models/staging/stg_weather_hourly.sql` - dbt staging model to create (SQL transformations, derived fields, WMO code mapping)
- `/Users/chris/Projects/city-cycles/orchestrator/main.py` - Orchestrator to modify (integrate weather extraction into pipeline flow)