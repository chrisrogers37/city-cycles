from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd

@dataclass
class LondonLegacyBikeShareRecord:
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

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any], source_file: str) -> 'LondonLegacyBikeShareRecord':
        return cls(
            rental_id=str(row['Rental Id']),
            bike_id=str(row['Bike Id']),
            start_date=datetime.strptime(row['Start Date'], "%d/%m/%Y %H:%M"),
            end_date=datetime.strptime(row['End Date'], "%d/%m/%Y %H:%M"),
            duration=int(row['Duration']),
            start_station_id=str(row['StartStation Id']),
            start_station_name=row['StartStation Name'],
            end_station_id=str(row['EndStation Id']),
            end_station_name=row['EndStation Name'],
            source_file=source_file
        )

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
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
class LondonModernBikeShareRecord:
    """Model for London bike share data from 2021+ (modern schema)."""
    number: str
    bike_number: str
    bike_model: str
    start_date: datetime
    end_date: datetime
    total_duration: str
    total_duration_ms: int
    start_station_number: str
    start_station: str
    end_station_number: str
    end_station: str
    source_file: str

    @classmethod
    def from_csv_row(cls, row: Dict[str, Any], source_file: str) -> 'LondonModernBikeShareRecord':
        return cls(
            number=str(row['Number']),
            bike_number=str(row['Bike number']),
            bike_model=row['Bike model'],
            start_date=datetime.strptime(row['Start date'], "%Y-%m-%d %H:%M"),
            end_date=datetime.strptime(row['End date'], "%Y-%m-%d %H:%M"),
            total_duration=row['Total duration'],
            total_duration_ms=int(row['Total duration (ms)']),
            start_station_number=str(row['Start station number']),
            start_station=row['Start station'],
            end_station_number=str(row['End station number']),
            end_station=row['End station'],
            source_file=source_file
        )

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
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
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M").dt.strftime("%Y-%m-%d %H:%M:%S")
        return df[list(cls.__dataclass_fields__.keys())]

def get_london_model_class(year: int) -> type:
    """Return the appropriate model class based on the year of the data."""
    if year >= 2021:
        return LondonModernBikeShareRecord
    else:
        return LondonLegacyBikeShareRecord 