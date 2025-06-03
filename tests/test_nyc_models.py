import pandas as pd
import os
from data_models.nyc_bike import NYCModernBikeShareRecord, NYCLegacyBikeShareRecord, get_nyc_model_class

def test_modern_format():
    """Test the modern format (2020+) model with a sample CSV."""
    path = os.path.join(os.path.dirname(__file__), 'nyc_sample_data', '202404-citibike-tripdata.csv')
    df = pd.read_csv(path)
    row = df.iloc[0].to_dict()
    model_class = get_nyc_model_class(2024)
    record = model_class.from_csv_row(row)
    print("\nModern Format Test (2024):")
    print(f"Ride ID: {record.ride_id}")
    print(f"Rideable Type: {record.rideable_type}")
    print(f"Start Time: {record.start_time} (type: {type(record.start_time)})")
    print(f"End Time: {record.end_time} (type: {type(record.end_time)})")
    print(f"User Type: {record.user_type}")
    print(f"Start Station: {record.start_station_name}")
    print(f"End Station: {record.end_station_name}")

def test_legacy_format():
    """Test the legacy format (2019) model with a sample CSV."""
    path = os.path.join(os.path.dirname(__file__), 'nyc_sample_data', '201907-citibike-tripdata_2.csv')
    df = pd.read_csv(path)
    row = df.iloc[0].to_dict()
    model_class = get_nyc_model_class(2019)
    record = model_class.from_csv_row(row)
    print("\nLegacy Format Test (2019):")
    print(f"Bike ID: {record.bike_id}")
    print(f"Trip Duration: {record.trip_duration}")
    print(f"Start Time: {record.start_time} (type: {type(record.start_time)})")
    print(f"End Time: {record.end_time} (type: {type(record.end_time)})")
    print(f"User Type: {record.user_type}")
    print(f"Birth Year: {record.birth_year}")
    print(f"Gender: {record.gender}")
    print(f"Start Station: {record.start_station_name}")
    print(f"End Station: {record.end_station_name}")

if __name__ == "__main__":
    test_modern_format()
    test_legacy_format() 