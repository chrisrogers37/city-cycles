"""Similar-day API routes — daily stats and hourly patterns for days like today."""

import logging
import os

import duckdb

from fastapi import APIRouter, HTTPException

from api.dependencies import CityParam, parquet_path, parquet_exists
from api.models.similar_day import (
    ClassificationResponse,
    HourlyBucketResponse,
    SimilarDayHourlyResponse,
    SimilarDayResponse,
)
from api.services.weather_bridge import get_city_weather, weather_to_conditions
from api import cache
from api.services.recommendation_engine import (
    classify_conditions,
    lookup_similar_day_stats,
    _current_month_and_day_type,
    _TEMP_BAND_TO_MART,
    _PRECIP_TO_MART,
)
from api.services.weather_service import WeatherAPIError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/similar-day", tags=["similar-day"])

SIMILAR_DAY_TTL = 1800  # 30 minutes
_SIMILAR_DAY_PARQUET = "mart_similar_day_stats.parquet"
_HOURLY_PATTERNS_PARQUET = "mart_hourly_patterns_summary.parquet"


def _get_classification(city: str) -> tuple[str, str, int, str]:
    """Get mart-mapped classification for current conditions.

    Returns (temperature_band, precipitation_intensity, month, day_type).
    """
    weather = get_city_weather(city)
    conditions = weather_to_conditions(weather)
    classified = classify_conditions(conditions)

    mart_temp = _TEMP_BAND_TO_MART.get(
        classified.temperature_band.value, classified.temperature_band.value
    )
    mart_precip = _PRECIP_TO_MART.get(
        classified.precipitation_intensity.value,
        classified.precipitation_intensity.value,
    )
    month, day_type = _current_month_and_day_type()
    return mart_temp, mart_precip, month, day_type


@router.get("/{city}", response_model=SimilarDayResponse)
def get_similar_day(city: CityParam):
    """Similar day stats for current conditions (daily grain)."""
    cache_key = f"similar-day:{city.value}"
    cached = cache.get(cache_key, SIMILAR_DAY_TTL)
    if cached is not None:
        return cached

    try:
        mart_temp, mart_precip, month, day_type = _get_classification(city.value)
    except WeatherAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    result = lookup_similar_day_stats(
        location=city.value,
        month=month,
        day_type=day_type,
        temperature_band=mart_temp,
        precipitation_intensity=mart_precip,
    )

    response = SimilarDayResponse(
        avg_daily_rides=result.avg_daily_rides,
        pct_change_vs_overall=result.pct_change_vs_overall,
        avg_duration_minutes=result.avg_duration_minutes,
        duration_pct_change_vs_overall=result.duration_pct_change_vs_overall,
        peak_hour_start=result.peak_hour_start,
        peak_hour_end=result.peak_hour_end,
        sample_days=result.sample_days,
        month=result.month,
        day_type=result.day_type,
        temperature_band=result.temperature_band,
        precipitation_intensity=result.precipitation_intensity,
    )

    cache.set(cache_key, response)
    return response


@router.get("/{city}/hourly", response_model=SimilarDayHourlyResponse)
def get_similar_day_hourly(city: CityParam):
    """Hourly ride patterns for similar days (24 rows)."""
    cache_key = f"similar-day-hourly:{city.value}"
    cached = cache.get(cache_key, SIMILAR_DAY_TTL)
    if cached is not None:
        return cached

    try:
        mart_temp, mart_precip, month, day_type = _get_classification(city.value)
    except WeatherAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    similar_path = parquet_path(_SIMILAR_DAY_PARQUET)
    overall_path = parquet_path(_HOURLY_PATTERNS_PARQUET)

    if not os.path.exists(similar_path):
        raise HTTPException(status_code=404, detail="Similar day data not available")

    conn = duckdb.connect(":memory:")

    # Hourly grain from mart_similar_day_stats
    similar_query = f"""
        SELECT
            hour_of_day,
            avg_daily_rides,
            avg_duration_minutes,
            avg_member_rides,
            avg_casual_rides,
            sample_days
        FROM '{similar_path}'
        WHERE grain = 'hourly'
          AND location = $1
          AND month_num = $2
          AND day_type = $3
          AND temperature_band = $4
          AND precipitation_intensity = $5
        ORDER BY hour_of_day
    """
    similar_df = conn.execute(
        similar_query, [city.value, month, day_type, mart_temp, mart_precip]
    ).fetchdf()

    # Overall average from hourly patterns summary
    overall_df = None
    if os.path.exists(overall_path):
        overall_query = f"""
            SELECT hour_of_day, ride_count as avg_rides
            FROM '{overall_path}'
            WHERE location = $1
            ORDER BY hour_of_day
        """
        overall_df = conn.execute(overall_query, [city.value]).fetchdf()

    overall_lookup = {}
    if overall_df is not None and not overall_df.empty:
        overall_lookup = dict(zip(overall_df["hour_of_day"], overall_df["avg_rides"]))

    hours = []
    for _, row in similar_df.iterrows():
        h = int(row["hour_of_day"])
        hours.append(
            HourlyBucketResponse(
                hour_of_day=h,
                similar_day_avg_rides=float(row["avg_daily_rides"]),
                overall_avg_rides=overall_lookup.get(h),
                similar_day_avg_duration=(
                    float(row["avg_duration_minutes"])
                    if row.get("avg_duration_minutes") is not None
                    else None
                ),
                member_rides=(
                    float(row["avg_member_rides"])
                    if row.get("avg_member_rides") is not None
                    else None
                ),
                casual_rides=(
                    float(row["avg_casual_rides"])
                    if row.get("avg_casual_rides") is not None
                    else None
                ),
                sample_days=(
                    int(row["sample_days"])
                    if row.get("sample_days") is not None
                    else None
                ),
            )
        )

    response = SimilarDayHourlyResponse(
        hours=hours,
        classification=ClassificationResponse(
            temperature_band=mart_temp,
            precipitation_intensity=mart_precip,
            month_num=month,
            day_type=day_type,
        ),
    )

    cache.set(cache_key, response)
    return response
