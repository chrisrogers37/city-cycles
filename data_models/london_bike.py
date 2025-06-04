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

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns for legacy London format."""
        required_columns = [
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
        missing_columns = [col for col in required_columns if col not in df.columns]
        return not missing_columns

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

    @classmethod
    def validate_schema(cls, df: pd.DataFrame) -> bool:
        """Validate if the dataframe contains all required columns for modern London format."""
        required_columns = [
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
        missing_columns = [col for col in required_columns if col not in df.columns]
        return not missing_columns

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
        for col in ["start_date", "end_date"]:
            # Modern format uses YYYY-MM-DD HH:MM
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M").dt.strftime("%Y-%m-%d %H:%M:%S")
        return df[list(cls.__dataclass_fields__.keys())] 