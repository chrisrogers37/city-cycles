from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class NYCLegacyBikeShareRecord:
    tripduration: int
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
    bikeid: str
    usertype: str
    birth_year: Optional[int]
    gender: Optional[int]

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any]) -> 'NYCLegacyBikeShareRecord':
        return cls(
            tripduration=int(row['tripduration']),
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
            bikeid=str(row['bikeid']),
            usertype=row['usertype'],
            birth_year=int(row['birth year']) if row.get('birth year') and str(row['birth year']).isdigit() else None,
            gender=int(row['gender']) if row.get('gender') and str(row['gender']).isdigit() else None
        )

@dataclass
class NYCModernBikeShareRecord:
    ride_id: str
    rideable_type: str
    started_at: str
    ended_at: str
    start_station_name: str
    start_station_id: str
    end_station_name: str
    end_station_id: str
    start_lat: float
    start_lng: float
    end_lat: float
    end_lng: float
    member_casual: str

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any]) -> 'NYCModernBikeShareRecord':
        return cls(
            ride_id=row['ride_id'],
            rideable_type=row['rideable_type'],
            started_at=row['started_at'],
            ended_at=row['ended_at'],
            start_station_name=row['start_station_name'],
            start_station_id=str(row['start_station_id']),
            end_station_name=row['end_station_name'],
            end_station_id=str(row['end_station_id']),
            start_lat=float(row['start_lat']),
            start_lng=float(row['start_lng']),
            end_lat=float(row['end_lat']),
            end_lng=float(row['end_lng']),
            member_casual=row['member_casual']
        )

def get_nyc_model_class(year: int) -> type:
    """Return the appropriate model class based on the year of the data."""
    if year >= 2020:
        return NYCModernBikeShareRecord
    else:
        return NYCLegacyBikeShareRecord 