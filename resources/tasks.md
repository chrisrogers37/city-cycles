# 🛠️ Step-by-Step MVP Task Plan for Bike Sharing Analytics

---

## 📦 Phase 1: Local Project Setup

### 1. Initialize Git repository
- [x] Create a new Git repo: `city-cycles`
- [x] Add `.gitignore` for Python, dbt, and secrets files

### 2. Set up Python virtual environment
- [x] `python -m venv venv`
- [x] Install base packages: `pandas`, `requests`, `python-dotenv`, `pyarrow`
- [x] Create `requirements.txt` with package versions
- [x] Confirm activation and Python compatibility

### 3. Create initial folder structure
- [x] Generate folders: `data_ingestion`, `data_models`, `db`, `dbt_project`, `dashboards`, `config`, `scripts`, `tests`

---

## ☁️ Phase 2: Cloud Environment Setup

### 4. Provision AWS S3 bucket for raw and intermediate data
- [x] Create S3 bucket for project (`city-cycles-data-ctr37`)
- [x] Set up permissions for EC2 to access S3 (IAM role: `city-cycles-ec2-s3-role`)

### 5. Provision Amazon RDS (Postgres) instance
- [x] Create RDS instance (Postgres) in AWS and database `citycycles` (connection tested)
- [x] Store DB credentials in `config/secrets.toml`

### 6. Provision AWS EC2 instance for ETL
- [x] Launch EC2 (Amazon Linux 2, t2.micro) in same region as RDS and S3, attach IAM role, and connect via SSH (RDS connectivity tested)

### 7. Set up environment on EC2
- [x] Install Python, pip, and required system packages
- [x] Clone repo and set up virtualenv
- [x] Copy `requirements.txt` and install dependencies

### 8. Configure secrets and environment variables on EC2
- [x] Securely copy `config/secrets.toml` to EC2
- [x] Set up environment variables for DB and S3 connection

---

## 🌍 Phase 3: Data Ingestion & Staging (Cloud-Native)

### 9. Write and test NYC CitiBike ingestion function
- [x] Implement function in `data_ingestion/nyc.py` to list, download, and upload all NYC CitiBike zip files from public S3 to your S3 bucket
- [x] The same function must, after downloading each zip file to EC2, unzip the archive, upload all extracted CSV(s) to S3 (e.g., under `nyc_csv/`), and clean up local files—all as part of a single ingestion process

### 10. Write and test London Santander ingestion function
- [x] Implement function in `data_ingestion/london.py` to list, download, and upload all London Santander zip files to your S3 bucket
- [x] The same function must, after downloading each zip file to EC2, unzip the archive, upload all extracted CSV(s) to S3 (e.g., under `london_csv/`), and clean up local files—all as part of a single ingestion process

---

## 🧱 Phase 4: Data Modeling & Loading

### 12. Inspect and document NYC data schema
- [x] Load CSV from S3 with `pandas`
- [x] Record column names and types
- [x] Save structure to `data_models/nyc_bike.py` as a Pydantic or dataclass model
- [x] Data models validated with untouched raw files and a test script moved to /tests

### 13. Inspect and document London data schema
- [x] Load CSV from S3 with `pandas`
- [x] Record column names and types
- [x] Save structure to `data_models/london_bike.py`

### 14. Write table DDLs for RDS
- [x] Create `db/init.sql` with DDL for 4 initial staging tables:
  - [x] `raw_nyc_legacy`
  - [x] `raw_nyc_modern`
  - [x] `raw_london_legacy`
  - [x] `raw_london_modern`

### 15. Write Python insert scripts for loading from S3 to RDS
- [x] In `db/load.py`, write insert functions:
  - [x] `insert_nyc_legacy(df)`
  - [x] `insert_nyc_modern(df)`
  - [x] `insert_london_legacy(df)`
  - [x] `insert_london_modern(df)`


### 16. Test raw load for one sample of each data type from S3 to RDS
- [x] Load 1 week NYC (legacy and modern), 1 week London (legacy and modern) from S3

### 16.5. Major Refactor: Model-Driven ETL and Automation
- [x] Refactored ETL pipeline to use a robust, extensible, and maintainable model-driven approach:
  - [x] Centralized all S3-to-database loading logic in the BaseBikeShareRecord class and its subclasses
  - [x] Implemented a model registry and robust file-to-model assignment using matches_file methods
  - [x] Added schema alignment, column validation, and type conversion in each data model
  - [x] Automated DDL generation from data model classes via get_schema_sql
  - [x] Added create_table and create_all_tables methods to execute DDLs directly from Python
  - [x] Created batch loading scripts for all files in a prefix or all prefixes, with improved error handling and logging
  - [x] Removed legacy scripts (load.py, static init.sql) in favor of dynamic, model-driven operations

---

## 🧪 Phase 5: dbt Transformation

### 17. Initialize dbt project and add Postgres profile
- [x] `dbt init dbt_project`
- [x] Add Postgres profile
- [x] Configure database connection

### 18. Create staging models
- [x] `stg_nyc_legacy.sql`, `stg_nyc_modern.sql`, `stg_london_legacy.sql`, `stg_london_modern.sql`
- [x] Implement proper data type casting
- [x] Add metadata fields (location, schema_version, dbt_updated_at)
- [x] Create unique ride IDs
- [x] Add appropriate indexes
- [x] Implement incremental processing
- [x] Test model materializations and basic transforms

### 19. Create intermediate models
- [x] `int_nyc_rides.sql`, `int_london_rides.sql`
- [x] Combine modern and legacy data
- [x] Standardize column names and types
- [x] Add appropriate indexes
- [x] Implement incremental processing
- [x] Test model materializations

### 20. Create metrics models
- [x] Create city-specific aggregations
- [x] Add time-based analysis
- [x] Create comparison metrics
- [ ] Add data quality tests
- [ ] Implement documentation
- [ ] Add data lineage tracking

---

## 🔁 Phase 6: Automation & Maintenance

### 21. Automate ETL execution
- [ ] Add a cron job (or systemd timer) to run ETL script on schedule (reading from S3)
- [ ] (Optional) Set up logging and error notification

### 22. (Optional) Clean up old files in S3
- [ ] Add script/cron to delete/archive old zip/CSV files in S3 to save storage

---

## 📊 Phase 7: Dashboard Reporting

### 23. Choose BI tool and connect to DB/dbt outputs
- [ ] Tableau (recommended), PowerBI, or Streamlit

### 24. Build city-specific dashboards
- [ ] London: ridership trends, top stations, durations
- [ ] NYC: same structure

### 25. Build comparison dashboard
- [ ] YoY growth, COVID drop + rebound, seasonal overlays

---

## 🧼 Phase 8: Packaging

### 26. Export visuals to PDF
- [ ] Add screenshots to final `submission.pdf`

### 27. Write summary of architecture
- [ ] Add commentary on trade-offs, automation, future scaling

---

## ✅ MVP Complete

---

## 🌍 Phase 9: Weather & COVID Data Ingestion (Nice-to-Have)

### 28. Weather Data Ingestion
- [ ] Implement function in `data_ingestion/weather.py` to fetch weather data, store as CSV in S3
- [ ] Ensure all intermediate files are cleaned up from EC2 after upload

### 29. COVID Data Ingestion
- [ ] Implement function in `data_ingestion/covid.py` to fetch COVID data, store as CSV in S3
- [ ] Ensure all intermediate files are cleaned up from EC2 after upload

### 30. Update dbt models to include weather and COVID data
- [ ] Update staging models to include weather and COVID data
- [ ] Update summary models to include weather and COVID data
- [ ] Update comparison model to include weather and COVID data

## Notes
- All staging models now use proper data types and indexes
- NYC Modern remains as a view while raw table is being indexed
- Other staging models are materialized as incremental tables
- Intermediate models combine modern and legacy data with proper indexing
- Analytics layer is in progress with metrics models created
