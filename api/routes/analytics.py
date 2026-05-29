"""Analytics API routes — ride metrics, hourly patterns, weather correlation, stations."""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import CityParam, parquet_path, parquet_exists
from api.models.analytics import (
    DailyMetricsResponse,
    DurationTrendRow,
    HourlyPatternRow,
    HourlyWeatherImpactRow,
    MemberAnalysisRow,
    MonthlyTrendRow,
    StationGrowthRow,
    StationPerformanceRow,
    WeatherCorrelationPrecipRow,
    WeatherCorrelationTempRow,
    WeatherImpactRow,
)
from api.services.query_service import run_query_params
from api import cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/analytics", tags=["analytics"])

ANALYTICS_TTL = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Daily Metrics
# ---------------------------------------------------------------------------


@router.get("/{city}/daily-metrics", response_model=DailyMetricsResponse)
def get_daily_metrics(
    city: CityParam,
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    """Key ride metrics for a date range: total rides, avg daily, avg duration."""
    cache_key = f"daily-metrics:{city.value}:{start_date}:{end_date}"
    cached = cache.get(cache_key, ANALYTICS_TTL)
    if cached is not None:
        return cached

    mart = parquet_path("mart_daily_metrics_long.parquet")
    params = [city.value, start_date, end_date]

    total_query = f"""
    SELECT SUM(metric_value) as total_rides
    FROM '{mart}'
    WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
    """
    avg_daily_query = f"""
    SELECT AVG(daily_rides) as avg_daily_rides FROM (
        SELECT date, SUM(metric_value) as daily_rides
        FROM '{mart}'
        WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
        GROUP BY date
    ) t
    """
    avg_duration_query = f"""
    SELECT SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
           NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0)
           AS avg_ride_duration_minutes
    FROM '{mart}'
    WHERE location = $1 AND date BETWEEN $2 AND $3
    """

    try:
        total_df = run_query_params(total_query, params)
        avg_daily_df = run_query_params(avg_daily_query, params)
        avg_dur_df = run_query_params(avg_duration_query, params)
    except Exception as e:
        logger.exception("Error querying daily metrics")
        raise HTTPException(status_code=500, detail=str(e))

    total_rides = float(total_df["total_rides"].iloc[0] or 0)
    avg_daily = float(avg_daily_df["avg_daily_rides"].iloc[0] or 0)
    avg_dur_val = avg_dur_df["avg_ride_duration_minutes"].iloc[0]
    avg_duration = float(avg_dur_val) if avg_dur_val is not None else None

    response = DailyMetricsResponse(
        total_rides=total_rides,
        avg_daily_rides=avg_daily,
        avg_ride_duration_minutes=avg_duration,
    )
    cache.set(cache_key, response)
    return response


# ---------------------------------------------------------------------------
# Monthly Trends (rides + duration)
# ---------------------------------------------------------------------------


@router.get("/{city}/monthly-trends", response_model=list[MonthlyTrendRow])
def get_monthly_trends(
    city: CityParam,
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    aggregation: str = Query("avg", description="'avg' or 'sum'"),
):
    """Monthly ride trends overlayed by year."""
    agg_func = "AVG" if aggregation == "avg" else "SUM"
    mart = parquet_path("mart_daily_metrics_long.parquet")

    query = f"""
    SELECT EXTRACT(MONTH FROM date) AS month, year, {agg_func}(metric_value) as metric_value
    FROM '{mart}'
    WHERE location = $1 AND date BETWEEN $2 AND $3 AND metric_name = 'total_rides'
    GROUP BY month, year ORDER BY month, year
    """
    try:
        df = run_query_params(query, [city.value, start_date, end_date])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        MonthlyTrendRow(
            month=int(r["month"]),
            year=int(r["year"]),
            metric_value=float(r["metric_value"]),
        )
        for _, r in df.iterrows()
    ]


@router.get("/{city}/duration-trends", response_model=list[DurationTrendRow])
def get_duration_trends(
    city: CityParam,
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    """Monthly average trip duration overlayed by year."""
    mart = parquet_path("mart_daily_metrics_long.parquet")

    query = f"""
    SELECT EXTRACT(MONTH FROM date) AS month, year,
      SUM(CASE WHEN metric_name = 'total_minutes_biked' THEN metric_value ELSE 0 END) /
        NULLIF(SUM(CASE WHEN metric_name = 'total_rides' THEN metric_value ELSE 0 END), 0) AS avg_duration
    FROM '{mart}'
    WHERE location = $1 AND date BETWEEN $2 AND $3
      AND metric_name IN ('total_minutes_biked', 'total_rides')
    GROUP BY month, year ORDER BY month, year
    """
    try:
        df = run_query_params(query, [city.value, start_date, end_date])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        DurationTrendRow(
            month=int(r["month"]),
            year=int(r["year"]),
            avg_duration=float(r["avg_duration"])
            if r["avg_duration"] is not None
            else None,
        )
        for _, r in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# Hourly Patterns
# ---------------------------------------------------------------------------


@router.get("/{city}/hourly-patterns", response_model=list[HourlyPatternRow])
def get_hourly_patterns(city: CityParam):
    """Ride distribution by hour of day."""
    if not parquet_exists("mart_hourly_patterns_summary.parquet"):
        return []

    mart = parquet_path("mart_hourly_patterns_summary.parquet")
    query = f"""
    SELECT hour_of_day, ride_count
    FROM '{mart}'
    WHERE location = $1
    ORDER BY hour_of_day
    """
    try:
        df = run_query_params(query, [city.value])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        HourlyPatternRow(
            hour_of_day=int(r["hour_of_day"]), ride_count=float(r["ride_count"])
        )
        for _, r in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# Weather Correlation
# ---------------------------------------------------------------------------


@router.get(
    "/{city}/weather-correlation/temperature",
    response_model=list[WeatherCorrelationTempRow],
)
def get_weather_correlation_temp(city: CityParam):
    """Average daily rides by temperature range."""
    if not parquet_exists("mart_weather_ride_correlation.parquet"):
        return []

    mart = parquet_path("mart_weather_ride_correlation.parquet")
    query = f"""
    SELECT
        CASE
            WHEN temperature_celsius < 0 THEN 'Below 0°C'
            WHEN temperature_celsius < 5 THEN '0-5°C'
            WHEN temperature_celsius < 10 THEN '5-10°C'
            WHEN temperature_celsius < 15 THEN '10-15°C'
            WHEN temperature_celsius < 20 THEN '15-20°C'
            WHEN temperature_celsius < 25 THEN '20-25°C'
            WHEN temperature_celsius < 30 THEN '25-30°C'
            ELSE '30°C+'
        END as temp_range,
        MIN(temperature_celsius) as temp_sort,
        round(avg(ride_count), 0) as avg_rides,
        count(*) as days_observed
    FROM '{mart}'
    WHERE location = $1
    GROUP BY temp_range
    ORDER BY temp_sort
    """
    try:
        df = run_query_params(query, [city.value])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        WeatherCorrelationTempRow(
            temp_range=r["temp_range"],
            avg_rides=float(r["avg_rides"]),
            days_observed=int(r["days_observed"]),
        )
        for _, r in df.iterrows()
    ]


@router.get(
    "/{city}/weather-correlation/precipitation",
    response_model=list[WeatherCorrelationPrecipRow],
)
def get_weather_correlation_precip(city: CityParam):
    """Average daily rides by precipitation category."""
    if not parquet_exists("mart_weather_ride_correlation.parquet"):
        return []

    mart = parquet_path("mart_weather_ride_correlation.parquet")
    query = f"""
    SELECT
        CASE
            WHEN precipitation_mm = 0 THEN 'Dry'
            WHEN precipitation_mm < 2 THEN 'Light (0-2mm)'
            WHEN precipitation_mm < 10 THEN 'Moderate (2-10mm)'
            ELSE 'Heavy (10mm+)'
        END as precip_category,
        MIN(precipitation_mm) as precip_sort,
        round(avg(ride_count), 0) as avg_rides,
        count(*) as days_observed
    FROM '{mart}'
    WHERE location = $1
    GROUP BY precip_category
    ORDER BY precip_sort
    """
    try:
        df = run_query_params(query, [city.value])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        WeatherCorrelationPrecipRow(
            precip_category=r["precip_category"],
            avg_rides=float(r["avg_rides"]),
            days_observed=int(r["days_observed"]),
        )
        for _, r in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# Weather Impact
# ---------------------------------------------------------------------------


@router.get("/{city}/weather-impact", response_model=list[WeatherImpactRow])
def get_weather_impact(city: CityParam, conditions: Optional[str] = None):
    """Weather condition impact — % change vs clear per condition."""
    if not parquet_exists("mart_weather_impact_summary.parquet"):
        return []

    mart = parquet_path("mart_weather_impact_summary.parquet")
    query = f"""
    SELECT dimension_value as weather_condition,
           round(avg(pct_change_rides_vs_clear), 1) as pct_change,
           round(avg(avg_rides), 0) as avg_rides,
           sum(observation_count) as total_observations
    FROM '{mart}'
    WHERE location = $1
      AND dimension_type = 'weather_condition'
      AND dimension_value != 'clear'
    GROUP BY dimension_value
    ORDER BY pct_change
    """
    try:
        df = run_query_params(query, [city.value])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = [
        WeatherImpactRow(
            weather_condition=r["weather_condition"],
            pct_change=float(r["pct_change"]),
            avg_rides=float(r["avg_rides"]),
            total_observations=int(r["total_observations"]),
        )
        for _, r in df.iterrows()
    ]

    if conditions:
        allowed = {c.strip() for c in conditions.split(",")}
        rows = [r for r in rows if r.weather_condition in allowed]

    return rows


@router.get(
    "/{city}/weather-impact/hourly", response_model=list[HourlyWeatherImpactRow]
)
def get_hourly_weather_impact(city: CityParam):
    """Weather impact by hour — rain, snow, fog % change vs clear."""
    if not parquet_exists("mart_weather_impact_summary.parquet"):
        return []

    mart = parquet_path("mart_weather_impact_summary.parquet")
    query = f"""
    SELECT hour_of_day,
           round(avg(CASE WHEN dimension_value = 'rain' THEN pct_change_rides_vs_clear END), 1) as rain_impact,
           round(avg(CASE WHEN dimension_value = 'snow' THEN pct_change_rides_vs_clear END), 1) as snow_impact,
           round(avg(CASE WHEN dimension_value = 'fog' THEN pct_change_rides_vs_clear END), 1) as fog_impact
    FROM '{mart}'
    WHERE location = $1 AND dimension_type = 'weather_condition'
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    try:
        df = run_query_params(query, [city.value])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        HourlyWeatherImpactRow(
            hour_of_day=int(r["hour_of_day"]),
            rain_impact=float(r["rain_impact"])
            if r["rain_impact"] is not None
            else None,
            snow_impact=float(r["snow_impact"])
            if r["snow_impact"] is not None
            else None,
            fog_impact=float(r["fog_impact"]) if r["fog_impact"] is not None else None,
        )
        for _, r in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# Station Performance
# ---------------------------------------------------------------------------


@router.get("/{city}/station-performance", response_model=list[StationPerformanceRow])
def get_station_performance(
    city: CityParam,
    weather_condition: str = Query(..., description="e.g. rain, snow, fog"),
    hour_start: int = Query(default=7),
    hour_end: int = Query(default=19),
    limit: int = Query(default=20, le=100),
):
    """Station weather resilience rankings."""
    swp = "mart_station_weather_performance.parquet"
    sd = "mart_station_directory.parquet"
    if not parquet_exists(swp) or not parquet_exists(sd):
        return []

    query = f"""
        SELECT s.station_id, d.station_name, d.latitude, d.longitude,
            round(avg(s.pct_change_vs_clear), 1) as avg_pct_change,
            sum(s.total_rides) as total_rides_in_condition,
            round(avg(s.avg_duration_minutes), 1) as avg_duration
        FROM '{parquet_path(swp)}' s
        JOIN '{parquet_path(sd)}' d
            ON s.location = d.location AND s.station_id = d.station_id
        WHERE s.location = $1 AND s.weather_condition = $2
          AND s.hour_of_day BETWEEN $3 AND $4
          AND s.pct_change_vs_clear IS NOT NULL
        GROUP BY s.station_id, d.station_name, d.latitude, d.longitude
        ORDER BY avg_pct_change DESC LIMIT $5
    """
    try:
        df = run_query_params(
            query, [city.value, weather_condition, hour_start, hour_end, limit]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        StationPerformanceRow(
            station_id=str(r["station_id"]),
            station_name=r["station_name"],
            latitude=float(r["latitude"]) if r["latitude"] is not None else None,
            longitude=float(r["longitude"]) if r["longitude"] is not None else None,
            avg_pct_change=float(r["avg_pct_change"]),
            total_rides_in_condition=int(r["total_rides_in_condition"]),
            avg_duration=float(r["avg_duration"]),
        )
        for _, r in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# Member Analysis (NYC only)
# ---------------------------------------------------------------------------


@router.get("/{city}/member-analysis", response_model=list[MemberAnalysisRow])
def get_member_analysis(
    city: CityParam,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """NYC member vs casual percentage over time. Returns 404 for London."""
    if city.value != "nyc":
        raise HTTPException(
            status_code=404, detail="Member analysis only available for NYC"
        )

    if not parquet_exists("mart_nyc_member_analysis.parquet"):
        return []

    mart = parquet_path("mart_nyc_member_analysis.parquet")

    if start_date and end_date:
        query = f"""
        SELECT month, member_percentage
        FROM '{mart}'
        WHERE month BETWEEN $1 AND $2
        ORDER BY month
        """
        params = [start_date, end_date]
    else:
        query = f"SELECT month, member_percentage FROM '{mart}' ORDER BY month"
        params = []

    try:
        df = run_query_params(query, params) if params else run_query_params(query, [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        MemberAnalysisRow(
            month=str(r["month"]), member_percentage=float(r["member_percentage"])
        )
        for _, r in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# Station Growth
# ---------------------------------------------------------------------------


@router.get("/{city}/station-growth", response_model=list[StationGrowthRow])
def get_station_growth(
    city: CityParam,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
):
    """Station count by year."""
    if not parquet_exists("mart_station_growth.parquet"):
        return []

    mart = parquet_path("mart_station_growth.parquet")

    if start_year and end_year:
        query = f"""
        SELECT year, station_count
        FROM '{mart}'
        WHERE location = $1 AND year BETWEEN $2 AND $3
        ORDER BY year
        """
        params = [city.value, start_year, end_year]
    else:
        query = f"""
        SELECT year, station_count
        FROM '{mart}'
        WHERE location = $1
        ORDER BY year
        """
        params = [city.value]

    try:
        df = run_query_params(query, params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        StationGrowthRow(year=int(r["year"]), station_count=int(r["station_count"]))
        for _, r in df.iterrows()
    ]
