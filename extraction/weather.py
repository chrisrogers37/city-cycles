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
        DataFrame with columns: [timestamp, temperature_2m, relative_humidity_2m,
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

    hourly = data["hourly"]
    df = pd.DataFrame(hourly)

    df["city"] = city
    df = df.rename(columns={"time": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    logger.info(f"Fetched {len(df)} hourly records for {city} ({start_date} to {end_date})")

    return df


def fetch_forecast_weather(city: str, past_days: int = 5) -> pd.DataFrame:
    """
    Fetch recent + forecast weather data from Open-Meteo Forecast API.

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

    # Add source_file column for lineage tracking (matches bike data pipeline pattern)
    df = df.copy()
    df["source_file"] = filename

    local_path = os.path.join(LOCAL_TMP_DIR, filename)

    try:
        df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
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
    Each run creates a date-stamped file (NOT idempotent by design -- dbt's
    source_file-based incremental logic needs new filenames to trigger
    reprocessing, and the unique_key handles dedup).

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

        # Add source_file column for lineage tracking (matches bike data pipeline pattern)
        df = df.copy()
        df["source_file"] = filename

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
