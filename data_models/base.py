from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class BaseBikeShareRecord:
    """Base class for bike sharing records across different years and cities."""
    
    # Common fields that appear in most datasets
    start_time: datetime
    end_time: datetime
    start_station_id: str
    start_station_name: str
    start_latitude: float
    start_longitude: float
    end_station_id: str
    end_station_name: str
    end_latitude: float
    end_longitude: float
    user_type: str  # member/casual or subscriber/customer
    
    # Optional fields that may not be present in all datasets
    ride_id: Optional[str] = None
    rideable_type: Optional[str] = None
    bike_id: Optional[str] = None
    trip_duration: Optional[int] = None
    birth_year: Optional[int] = None
    gender: Optional[int] = None
    
    @classmethod
    def from_csv_row(cls, row: dict) -> 'BaseBikeShareRecord':
        """Convert a CSV row to a bike share record. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement from_csv_row method")
    
    def to_dict(self) -> dict:
        """Convert the record to a dictionary for database insertion."""
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'start_station_id': self.start_station_id,
            'start_station_name': self.start_station_name,
            'start_latitude': self.start_latitude,
            'start_longitude': self.start_longitude,
            'end_station_id': self.end_station_id,
            'end_station_name': self.end_station_name,
            'end_latitude': self.end_latitude,
            'end_longitude': self.end_longitude,
            'user_type': self.user_type,
            'ride_id': self.ride_id,
            'rideable_type': self.rideable_type,
            'bike_id': self.bike_id,
            'trip_duration': self.trip_duration,
            'birth_year': self.birth_year,
            'gender': self.gender
        } 