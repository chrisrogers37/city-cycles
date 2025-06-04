# 🚲 Bike Sharing Data Architecture (NYC CitiBike & London Santander)

## 🧱 Overview

This architecture is designed to support an end-to-end analytics workflow for analyzing NYC CitiBike and London Santander bike-sharing data. It includes automated data ingestion, weather data enrichment, transformation via dbt, and dashboard reporting — all with scalability and modularity in mind.

---

## 📁 File & Folder Structure

```
city-cycles/
├── data_ingestion/
│   ├── __init__.py
│   ├── london.py                # London JourneyDataExtract scraper
│   ├── nyc.py                   # NYC CitiBike scraper
│   ├── weather.py               # Weather API connector (for both cities)
│   └── utils.py                 # Shared download and parsing utilities
│
├── data_models/
│   ├── __init__.py
│   ├── base.py                  # Base class for raw data schema definitions
│   ├── london_bike.py           # Raw schema for London data
│   └── nyc_bike.py              # Raw schema for NYC data
│
├── db/
│   ├── init.sql                 # DDL to initialize tables (raw & staging)
│   └── load.py                  # Logic to insert parsed data into database
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── marts/
│   │   │   ├── city_comparison.sql
│   │   │   ├── london_summary.sql
│   │   │   └── nyc_summary.sql
│   │   └── staging/
│   │       ├── stg_london.sql
│   │       └── stg_nyc.sql
│   └── seeds/
│       └── covid_phases.csv     # Manually annotated COVID lockdown phases
│
├── dashboards/
│   └── tableau_workbook.twbx    # Or PowerBI or Streamlit
│
├── config/
│   ├── secrets.toml             # API keys (weather, DB connection)
│   └── settings.py              # Constants (URL endpoints, etc.)
│
├── scripts/
│   ├── run_pipeline.py          # Full ETL execution script
│   └── run_dbt.sh               # Trigger dbt transformation
│
├── tests/
│   ├── test_data_validation.py  # Schema and data tests
│
└── README.md
```

---

## ⚙️ Architecture Components

### 1. **Data Ingestion Layer**
- **Raw data is first downloaded and staged in Amazon S3. S3 acts as the canonical raw data store.**
- **`data_ingestion/london.py`**:
  - Downloads and uploads weekly CSVs from TfL FTP to S3
  - Cleans dates and standardizes column names
- **`data_ingestion/nyc.py`**:
  - Downloads monthly CitiBike CSVs from S3 (NYC public bucket) and stages them in your S3 bucket
- **`data_ingestion/weather.py`**:
  - Pulls historical daily weather using OpenWeatherMap or Meteostat API and stages results in S3

All ingestion outputs are stored in S3 and parsed into pandas DataFrames, conforming to schemas defined in `data_models/`.

---

### 2. **Data Modeling Layer**
- **Raw schema definitions** live in `data_models/`
  - Ensures consistent field handling across ingestion
  - Supports validation and transformation compatibility

---

### 3. **Database & Storage**
- **Database:** Amazon RDS with PostgreSQL
  - Scalable, easy to query with dbt + BI tools
- **Tables:**
  - `raw_london_rides`, `raw_nyc_rides`, `raw_weather`
  - `stg_london`, `stg_nyc`, `weather_joined`
  - `mart_city_comparison`
- **S3:**
  - Canonical store for all raw and intermediate files
  - All ETL scripts read from S3, process data, and load to RDS

---

### 4. **Transformation via dbt**
- **Staging models:** Clean + join raw data
- **Marts:** Summarized aggregates per city + comparative metrics
- Includes `covid_phases.csv` for contextual annotations

---

### 5. **Reporting Layer**
- Choose one of:
  - Tableau Workbook (`.twbx`)
  - PowerBI Desktop file
  - Streamlit app (`dashboards/app.py`)
- 3 Pages Required:
  - NYC Trends Dashboard
  - London Trends Dashboard
  - Comparative View

---

## 🗃️ State & Orchestration

- **State Tracking:** Basic via last-modified file marker in S3; extendable with Prefect or Airflow for scheduling
- **Secrets Management:** `secrets.toml` (never commit to Git)
- **Pipeline Execution:** `scripts/run_pipeline.py` (run on EC2) → `run_dbt.sh` → Open dashboards
- **All ETL/testing is performed on EC2 using S3 as the input source.**

---

## 🧠 Recommendations

- Use S3 as the canonical raw data store to minimize tech debt and ensure production readiness
- Use `pyarrow` or `feather` for intermediate files to speed up processing
- Use `pytest` to test for data quality regressions
- Optional: Add Looker Studio or Metabase for cloud-native dashboarding

---

## ✅ Summary

This modular design allows for:
- Seamless ingestion from both cities
- Scalable transformation using dbt
- Extendability for weather & COVID enrichment
- Portable and replicable BI layer
- **Cloud-native, production-ready ETL with S3 as the data backbone**
