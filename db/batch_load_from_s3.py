import os
import sys
import re
import pandas as pd
from db.load import (
    download_csv_from_s3,
    insert_nyc_legacy,
    insert_nyc_modern,
    insert_london_legacy,
    insert_london_modern
)
from data_models.london_bike import get_london_model_class
from data_models.nyc_bike import get_nyc_model_class

# Usage: python db/batch_load_from_s3.py <s3_key>
def main():
    if len(sys.argv) != 2:
        print("Usage: python db/batch_load_from_s3.py <s3_key>")
        sys.exit(1)
    s3_key = sys.argv[1]
    filename = os.path.basename(s3_key)
    print(f"Processing file: {filename}")
    # Determine city
    if filename.lower().startswith("journeydataextract") or "london" in s3_key.lower():
        city = "london"
    elif "citibike" in filename.lower() or "nyc" in s3_key.lower():
        city = "nyc"
    else:
        print("Could not determine city from filename.")
        sys.exit(1)
    # Extract year from filename
    year_match = re.search(r"(20\d{2})", filename)
    if not year_match:
        print("Could not determine year from filename.")
        sys.exit(1)
    year = int(year_match.group(1))
    # Download and read CSV
    csv_buffer = download_csv_from_s3(s3_key)
    df = pd.read_csv(csv_buffer)
    df['source_file'] = filename
    # Route to correct loader using data model class for alignment
    if city == "london":
        model_class = get_london_model_class(year)
        df_aligned = model_class.from_dataframe(df, filename)
        if model_class.__name__ == "LondonLegacyBikeShareRecord":
            print(f"Detected London legacy schema for year {year}. Inserting to raw_london_legacy.")
            insert_london_legacy(df_aligned)
        else:
            print(f"Detected London modern schema for year {year}. Inserting to raw_london_modern.")
            insert_london_modern(df_aligned)
    elif city == "nyc":
        model_class = get_nyc_model_class(year)
        df_aligned = model_class.from_dataframe(df, filename)
        if model_class.__name__ == "NYCLegacyBikeShareRecord":
            print(f"Detected NYC legacy schema for year {year}. Inserting to raw_nyc_legacy.")
            insert_nyc_legacy(df_aligned)
        else:
            print(f"Detected NYC modern schema for year {year}. Inserting to raw_nyc_modern.")
            insert_nyc_modern(df_aligned)
    print("Done.")

if __name__ == "__main__":
    main() 