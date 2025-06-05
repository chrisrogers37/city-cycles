import os
import pandas as pd
from data_models.nyc_bike import NYCModernBikeShareRecord, NYCLegacyBikeShareRecord, get_nyc_model_class

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..", "data_models", "nyc_sample_data")

LEGACY_FILE = "201906-citibike-tripdata_3.csv"
MODERN_FILE = "202312-citibike-tripdata_3.csv"
N_ROWS = 5

def test_nyc_legacy():
    path = os.path.join(SAMPLE_DIR, LEGACY_FILE)
    df = pd.read_csv(path, nrows=N_ROWS)
    print(f"\nTesting legacy NYC model on {LEGACY_FILE}")
    for i, row in df.iterrows():
        record = NYCLegacyBikeShareRecord.from_csv_row(row)
        for field in NYCLegacyBikeShareRecord.__dataclass_fields__:
            assert hasattr(record, field), f"Missing field: {field}"
        print(record)

def test_nyc_modern():
    path = os.path.join(SAMPLE_DIR, MODERN_FILE)
    df = pd.read_csv(path, nrows=N_ROWS)
    print(f"\nTesting modern NYC model on {MODERN_FILE}")
    for i, row in df.iterrows():
        record = NYCModernBikeShareRecord.from_csv_row(row)
        for field in NYCModernBikeShareRecord.__dataclass_fields__:
            assert hasattr(record, field), f"Missing field: {field}"
        print(record)

if __name__ == "__main__":
    test_nyc_legacy()
    test_nyc_modern() 