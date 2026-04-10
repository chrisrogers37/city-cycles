"""Pydantic response models for analytics endpoints."""

from typing import Optional

from pydantic import BaseModel


class DailyMetricsResponse(BaseModel):
    total_rides: float
    avg_daily_rides: float
    avg_ride_duration_minutes: Optional[float] = None


class MonthlyTrendRow(BaseModel):
    month: int
    year: int
    metric_value: float


class DurationTrendRow(BaseModel):
    month: int
    year: int
    avg_duration: Optional[float] = None


class HourlyPatternRow(BaseModel):
    hour_of_day: int
    ride_count: float


class WeatherCorrelationTempRow(BaseModel):
    temp_range: str
    avg_rides: float
    days_observed: int


class WeatherCorrelationPrecipRow(BaseModel):
    precip_category: str
    avg_rides: float
    days_observed: int


class WeatherImpactRow(BaseModel):
    weather_condition: str
    pct_change: float
    avg_rides: float
    total_observations: int


class HourlyWeatherImpactRow(BaseModel):
    hour_of_day: int
    rain_impact: Optional[float] = None
    snow_impact: Optional[float] = None
    fog_impact: Optional[float] = None


class StationPerformanceRow(BaseModel):
    station_id: str
    station_name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    avg_pct_change: float
    total_rides_in_condition: int
    avg_duration: float


class MemberAnalysisRow(BaseModel):
    month: str
    member_percentage: float


class StationGrowthRow(BaseModel):
    year: int
    station_count: int
