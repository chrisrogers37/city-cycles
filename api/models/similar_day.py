"""Pydantic response models for similar-day endpoints."""

from typing import Optional

from pydantic import BaseModel


class SimilarDayResponse(BaseModel):
    avg_daily_rides: Optional[float] = None
    pct_change_vs_overall: Optional[float] = None
    avg_duration_minutes: Optional[float] = None
    duration_pct_change_vs_overall: Optional[float] = None
    peak_hour_start: Optional[int] = None
    peak_hour_end: Optional[int] = None
    sample_days: Optional[int] = None
    month: Optional[int] = None
    day_type: Optional[str] = None
    temperature_band: Optional[str] = None
    precipitation_intensity: Optional[str] = None


class HourlyBucketResponse(BaseModel):
    hour_of_day: int
    similar_day_avg_rides: float
    overall_avg_rides: Optional[float] = None
    similar_day_avg_duration: Optional[float] = None
    member_rides: Optional[float] = None
    casual_rides: Optional[float] = None
    sample_days: Optional[int] = None


class ClassificationResponse(BaseModel):
    temperature_band: str
    precipitation_intensity: str
    month_num: int
    day_type: str


class SimilarDayHourlyResponse(BaseModel):
    hours: list[HourlyBucketResponse]
    classification: ClassificationResponse
