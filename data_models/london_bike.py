from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class LondonLegacyBikeShareRecord:
    """Model for London bike share data from 2018-2020 (legacy schema)."""
    rental_id: str
    duration: int
    bike_id: str
    end_date: datetime
    end_station_id: str
    end_station_name: str
    start_date: datetime
    start_station_id: str
    start_station_name: str

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any]) -> 'LondonLegacyBikeShareRecord':
        return cls(
            rental_id=str(row['Rental Id']),
            duration=int(row['Duration']),
            bike_id=str(row['Bike Id']),
            end_date=datetime.strptime(row['End Date'], "%d/%m/%Y %H:%M"),
            end_station_id=str(row['EndStation Id']),
            end_station_name=row['EndStation Name'],
            start_date=datetime.strptime(row['Start Date'], "%d/%m/%Y %H:%M"),
            start_station_id=str(row['StartStation Id']),
            start_station_name=row['StartStation Name']
        )

@dataclass
class LondonModernBikeShareRecord:
    """Model for London bike share data from 2021+ (modern schema)."""
    number: str
    start_date: datetime
    start_station_number: str
    start_station: str
    end_date: datetime
    end_station_number: str
    end_station: str
    bike_number: str
    bike_model: str
    total_duration: str
    total_duration_ms: int

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any]) -> 'LondonModernBikeShareRecord':
        return cls(
            number=str(row['Number']),
            start_date=datetime.strptime(row['Start date'], "%Y-%m-%d %H:%M"),
            start_station_number=str(row['Start station number']),
            start_station=row['Start station'],
            end_date=datetime.strptime(row['End date'], "%Y-%m-%d %H:%M"),
            end_station_number=str(row['End station number']),
            end_station=row['End station'],
            bike_number=str(row['Bike number']),
            bike_model=row['Bike model'],
            total_duration=row['Total duration'],
            total_duration_ms=int(row['Total duration (ms)'])
        )

def get_london_model_class(year: int) -> type:
    """Return the appropriate model class based on the year of the data."""
    if year <= 2020:
        return LondonLegacyBikeShareRecord
    else:
        return LondonModernBikeShareRecord 