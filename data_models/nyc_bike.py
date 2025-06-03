from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseBikeShareRecord

class NYCModernBikeShareRecord(BaseBikeShareRecord):
    """Model for NYC bike share data from 2020 onwards (modern schema)."""
    ride_id: str
    rideable_type: str
    member_casual: str

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any]) -> 'NYCModernBikeShareRecord':
        return cls(
            ride_id=row['ride_id'],
            rideable_type=row['rideable_type'],
            start_time=datetime.fromisoformat(row['started_at'].replace('Z', '')),
            end_time=datetime.fromisoformat(row['ended_at'].replace('Z', '')),
            start_station_id=row['start_station_id'],
            start_station_name=row['start_station_name'],
            start_latitude=float(row['start_lat']),
            start_longitude=float(row['start_lng']),
            end_station_id=row['end_station_id'],
            end_station_name=row['end_station_name'],
            end_latitude=float(row['end_lat']),
            end_longitude=float(row['end_lng']),
            user_type=row['member_casual']
        )

class NYCLegacyBikeShareRecord(BaseBikeShareRecord):
    """Model for NYC bike share data from 2019 and earlier (legacy schema)."""
    bike_id: str
    trip_duration: int
    birth_year: Optional[int]
    gender: Optional[int]

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any]) -> 'NYCLegacyBikeShareRecord':
        return cls(
            bike_id=row['bikeid'],
            trip_duration=int(row['tripduration']),
            start_time=datetime.fromisoformat(row['starttime'].replace('Z', '')),
            end_time=datetime.fromisoformat(row['stoptime'].replace('Z', '')),
            start_station_id=str(row['start station id']),
            start_station_name=row['start station name'],
            start_latitude=float(row['start station latitude']),
            start_longitude=float(row['start station longitude']),
            end_station_id=str(row['end station id']),
            end_station_name=row['end station name'],
            end_latitude=float(row['end station latitude']),
            end_longitude=float(row['end station longitude']),
            user_type=row['usertype'],
            birth_year=int(row['birth year']) if row['birth year'] else None,
            gender=int(row['gender']) if row['gender'] else None
        )

def get_nyc_model_class(year: int) -> type:
    """Return the appropriate model class based on the year of the data."""
    if year >= 2020:
        return NYCModernBikeShareRecord
    else:
        return NYCLegacyBikeShareRecord 