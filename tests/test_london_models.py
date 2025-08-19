import os
import pandas as pd
from data_models.london_bike import LondonLegacyBikeShareRecord, LondonModernBikeShareRecord

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "london_test_data")

# Pick one legacy and one modern file
LEGACY_FILE = "193JourneyDataExtract18Dec2019-24Dec2019.csv"
MODERN_FILE = "360JourneyDataExtract06Mar2023-12Mar2023.csv"

N_ROWS = 5

def test_london_legacy():
    path = os.path.join(SAMPLE_DIR, LEGACY_FILE)
    df = pd.read_csv(path, nrows=N_ROWS)
    print(f"\nTesting legacy London model on {LEGACY_FILE}")
    
    # Test schema validation
    assert LondonLegacyBikeShareRecord.validate_schema(df)
    
    # Test transformation
    transformed = LondonLegacyBikeShareRecord.to_dataframe(df, "test.csv")
    for field in LondonLegacyBikeShareRecord.__dataclass_fields__:
        assert field in transformed.columns, f"Missing field: {field}"
    print(f"Transformed {len(transformed)} rows successfully")

def test_london_modern():
    path = os.path.join(SAMPLE_DIR, MODERN_FILE)
    df = pd.read_csv(path, nrows=N_ROWS)
    print(f"\nTesting modern London model on {MODERN_FILE}")
    
    # Test schema validation
    assert LondonModernBikeShareRecord.validate_schema(df)
    
    # Test transformation
    transformed = LondonModernBikeShareRecord.to_dataframe(df, "test.csv")
    for field in LondonModernBikeShareRecord.__dataclass_fields__:
        assert field in transformed.columns, f"Missing field: {field}"
    print(f"Transformed {len(transformed)} rows successfully")

if __name__ == "__main__":
    test_london_legacy()
    test_london_modern() 