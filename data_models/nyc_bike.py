from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd

@dataclass
class NYCLegacyBikeShareRecord:
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

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any], source_file: str) -> 'NYCLegacyBikeShareRecord':
        return cls(
            tripduration=int(row['tripduration']),
            bikeid=str(row['bikeid']),
            starttime=row['starttime'],
            stoptime=row['stoptime'],
            start_station_id=str(row['start station id']),
            start_station_name=row['start station name'],
            start_station_latitude=float(row['start station latitude']),
            start_station_longitude=float(row['start station longitude']),
            end_station_id=str(row['end station id']),
            end_station_name=row['end station name'],
            end_station_latitude=float(row['end station latitude']),
            end_station_longitude=float(row['end station longitude']),
            usertype=row['usertype'],
            birth_year=int(row['birth year']) if row.get('birth year') and str(row['birth year']).isdigit() else None,
            gender=int(row['gender']) if row.get('gender') and str(row['gender']).isdigit() else None,
            source_file=source_file
        )

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

@dataclass
class NYCModernBikeShareRecord:
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

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any], source_file: str) -> 'NYCModernBikeShareRecord':
        return cls(
            ride_id=row['ride_id'],
            rideable_type=row['rideable_type'],
            started_at=row['started_at'],
            ended_at=row['ended_at'],
            start_station_id=str(row['start_station_id']),
            start_station_name=row['start_station_name'],
            end_station_id=str(row['end_station_id']),
            end_station_name=row['end_station_name'],
            start_lat=float(row['start_lat']),
            start_lng=float(row['start_lng']),
            end_lat=float(row['end_lat']),
            end_lng=float(row['end_lng']),
            member_casual=row['member_casual'],
            source_file=source_file
        )

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

def get_nyc_model_class(year: int) -> type:
    """Return the appropriate model class based on the year of the data."""
    if year >= 2020:
        return NYCModernBikeShareRecord
    else:
        return NYCLegacyBikeShareRecord 