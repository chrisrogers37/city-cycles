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

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

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
