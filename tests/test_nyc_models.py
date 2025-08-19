import os
import pandas as pd
from data_models.nyc_bike import NYCModernBikeShareRecord, NYCLegacyBikeShareRecord

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "nyc_test_data")

LEGACY_FILE = "201906-citibike-tripdata_3.csv"
MODERN_FILE = "202312-citibike-tripdata_3.csv"
N_ROWS = 5

def test_nyc_legacy():
    path = os.path.join(SAMPLE_DIR, LEGACY_FILE)
    df = pd.read_csv(path, nrows=N_ROWS)
    print(f"\nTesting legacy NYC model on {LEGACY_FILE}")
    
    # Test schema validation
    assert NYCLegacyBikeShareRecord.validate_schema(df)
    
    # Test transformation
    transformed = NYCLegacyBikeShareRecord.to_dataframe(df, "test.csv")
    for field in NYCLegacyBikeShareRecord.__dataclass_fields__:
        assert field in transformed.columns, f"Missing field: {field}"
    print(f"Transformed {len(transformed)} rows successfully")

def test_nyc_modern():
    path = os.path.join(SAMPLE_DIR, MODERN_FILE)
    df = pd.read_csv(path, nrows=N_ROWS)
    print(f"\nTesting modern NYC model on {MODERN_FILE}")
    
    # Test schema validation
    assert NYCModernBikeShareRecord.validate_schema(df)
    
    # Test transformation
    transformed = NYCModernBikeShareRecord.to_dataframe(df, "test.csv")
    for field in NYCModernBikeShareRecord.__dataclass_fields__:
        assert field in transformed.columns, f"Missing field: {field}"
    print(f"Transformed {len(transformed)} rows successfully")

if __name__ == "__main__":
    test_nyc_legacy()
    test_nyc_modern() 