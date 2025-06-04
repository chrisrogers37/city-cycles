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
│   ├── nyc_bike.py              # Raw schema for NYC data
│   └── README.md               # Documentation for data models
│
├── db/
│   ├── init_tables.py          # Dynamic table initialization from models
│   ├── batch_load_from_s3.py   # Batch loading script for specific prefix
│   └── batch_load_all_from_s3.py # Load all prefixes
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
├── resources/
│   ├── architecture.md         # This file - system architecture documentation
│   ├── tasks.md               # Project tasks and progress tracking
│   └── playwright_recommendations.md # Playwright-specific guidance
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
  - Uses Playwright to download weekly CSVs from TfL website
  - Implements robust download handling with retries and validation
  - Uploads to S3 with standardized naming
- **`data_ingestion/nyc.py`**:
  - Downloads monthly CitiBike CSVs from S3 (NYC public bucket)
  - Stages them in your S3 bucket with consistent naming
- **`data_ingestion/weather.py`**:
  - Pulls historical daily weather using OpenWeatherMap or Meteostat API
  - Stages results in S3 for later joining

All ingestion outputs are stored in S3 and parsed into pandas DataFrames, conforming to schemas defined in `data_models/`.

---

### 2. **Data Modeling Layer**
- **Model-Driven Architecture** in `data_models/`:
  - `BaseBikeShareRecord`: Abstract base class providing common functionality
  - Model registry for automatic file-to-model assignment
  - Schema validation and alignment
  - Dynamic DDL generation and table creation
  - Memory-efficient, chunked loading with progress tracking
- **Key Features**:
  - Automatic model assignment based on file patterns
  - Schema alignment and type conversion
  - S3 integration for file listing and downloading
  - Database loading with chunked processing
  - Progress and memory usage logging

---

### 3. **Database & Storage**
- **Database:** Amazon RDS with PostgreSQL
  - Scalable, easy to query with dbt + BI tools
- **Tables:**
  - Dynamically created from model definitions
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
