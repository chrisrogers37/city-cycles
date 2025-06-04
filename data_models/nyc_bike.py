from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
from data_models.base import BaseBikeShareRecord
import re

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

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        df = df.rename(columns={
            "tripduration": "tripduration",
            "bikeid": "bikeid",
            "starttime": "starttime",
            "stoptime": "stoptime",
            "start station id": "start_station_id",
            "start station name": "start_station_name",
            "start station latitude": "start_station_latitude",
            "start station longitude": "start_station_longitude",
            "end station id": "end_station_id",
            "end station name": "end_station_name",
            "end station latitude": "end_station_latitude",
            "end station longitude": "end_station_longitude",
            "usertype": "usertype",
            "birth year": "birth_year",
            "gender": "gender"
        })
        df["source_file"] = source_file
        return df[list(cls.__dataclass_fields__.keys())]

    @classmethod
    def matches_file(cls, filename: str, df_head=None) -> bool:
        # Match years 2019 and earlier, allowing for additional suffixes
        match = re.search(r"201[0-9]", filename)
        print(f"Checking file {filename} with pattern 201[0-9]: {bool(match)}")
        return bool(match)

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

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        df = df.rename(columns={
            "ride_id": "ride_id",
            "rideable_type": "rideable_type",
            "started_at": "started_at",
            "ended_at": "ended_at",
            "start_station_id": "start_station_id",
            "start_station_name": "start_station_name",
            "end_station_id": "end_station_id",
            "end_station_name": "end_station_name",
            "start_lat": "start_lat",
            "start_lng": "start_lng",
            "end_lat": "end_lat",
            "end_lng": "end_lng",
            "member_casual": "member_casual"
        })
        df["source_file"] = source_file
        return df[list(cls.__dataclass_fields__.keys())]

    @classmethod
    def matches_file(cls, filename: str, df_head=None) -> bool:
        # Match years 2020 and later
        match = re.search(r"(20(2[0-9]|[3-9][0-9]))", filename)
        return bool(match) 