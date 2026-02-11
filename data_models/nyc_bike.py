from dataclasses import dataclass
from typing import Optional
import pandas as pd
from data_models.base import BaseBikeShareRecord

@dataclass
class NYCLegacyBikeShareRecord(BaseBikeShareRecord):
    tripduration: int
    bikeid: str
    starttime: str
    stoptime: str
    start_station_id: str
    start_station_name: str
    start_station_latitude: float
    start_station_longitude: float
    end_station_id: str
    end_station_name: str
    end_station_latitude: float
    end_station_longitude: float
    usertype: str
    birth_year: Optional[int]
    gender: Optional[int]
    source_file: str

    staging_table = "raw_nyc_legacy"
    s3_prefix = "nyc_csv/"

    # Store required columns for detailed validation
    _required_columns = [
        "tripduration",
        "starttime",
        "stoptime",
        "start station id",
        "start station name",
        "start station latitude",
        "start station longitude",
        "end station id",
        "end station name",
        "end station latitude",
        "end station longitude",
        "bikeid",
        "usertype",
        "birth year",
        "gender"
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        # Only rename columns that have different names in source
        df = df.rename(columns={
            "start station id": "start_station_id",
            "start station name": "start_station_name",
            "start station latitude": "start_station_latitude",
            "start station longitude": "start_station_longitude",
            "end station id": "end_station_id",
            "end station name": "end_station_name",
            "end station latitude": "end_station_latitude",
            "end station longitude": "end_station_longitude",
            "birth year": "birth_year",
        })
        df["source_file"] = source_file
        return df[list(cls.__dataclass_fields__.keys())]

@dataclass
class NYCModernBikeShareRecord(BaseBikeShareRecord):
    ride_id: str
    rideable_type: str
    started_at: str
    ended_at: str
    start_station_id: str
    start_station_name: str
    end_station_id: str
    end_station_name: str
    start_lat: float
    start_lng: float
    end_lat: float
    end_lng: float
    member_casual: str
    source_file: str

    staging_table = "raw_nyc_modern"
    s3_prefix = "nyc_csv/"

    # Store required columns for detailed validation
    _required_columns = [
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual"
    ]

    @classmethod
    def to_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        # No column renames needed - modern schema uses correct names
        df["source_file"] = source_file
        
        # Ensure station IDs are treated as strings (handle alphanumeric values)
        for col in ["start_station_id", "end_station_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        # Handle potential data quality issues in coordinates
        for col in ["start_lat", "start_lng", "end_lat", "end_lng"]:
            if col in df.columns:
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df[list(cls.__dataclass_fields__.keys())] 