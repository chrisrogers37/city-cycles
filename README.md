# City Cycles Analytics

This project demonstrates a full-stack, automated analytics pipeline for comparing bike share systems in New York City (CitiBike) and London (Santander Cycles). It showcases robust engineering, cloud infrastructure, and modern analytics best practices and includes a roadmap for future development and enhancement.

---

## Project Overview

I built an end-to-end, automatable flow that:

- **Utilizes modern cloud infrastructure** 
- **Extracts and ingests data from multiple sources:**
- **Performs schema validation and data modeling:**
- **Transforms and unifies data with dbt:** 
- **Visualizes results in a modern dashboard:** 
- **Automates and documents the entire process:**

---

## Infrastructure

- **AWS S3:** Centralized storage for all raw and processed data.
- **AWS RDS (PostgreSQL):** Scalable, cloud-hosted analytics database.
- **AWS EC2 (Ubuntu):** Orchestration and processing environment.
- **Streamlit Cloud:** Free public hosting for the dashboard.

---

## Data Extraction

- **NYC:**  
  - Uses `boto3` to list and download zipped CSVs from the official S3 bucket.
  - Unzips and uploads files to the project S3 bucket.

- **London:**  
  - Uses Playwright to automate browser downloads from the TFL website (no direct S3 access).
  - Handles schema differences and uploads to S3.

---

## Data Modeling & Loading

- **Python data classes** represent each schema (legacy/modern, NYC/London).
- **Validation** ensures only well-formed data is loaded.
- **Automated table creation**: Table schemas are generated from the data models.
- **Batch loading**: Efficient, chunked inserts from S3 to PostgreSQL.

---

## Transformation & Analytics

- **dbt** is used to:
  - Standardize and clean raw data in staging models.
  - Combine legacy and modern data into unified intermediate tables.
  - Build flexible, long-format metrics marts for analytics and dashboarding.

---

## Dashboard

- **Plotly + Streamlit** power an interactive analytics dashboard:
  - City-specific and comparative views.
  - Flexible date filtering, per-capita toggles, and trend overlays.
  - KPIs, time series, and station growth visualizations.
- **Deployed on Streamlit Cloud** for public access.

---

## Additional Documentation

- See `data_models/README.md` for details on the data model architecture.
- See `resources/` for learnings, design notes, task flows, and architecture ideas that I accumulated along the way.

## Technologies Used

- **Python 3.8+** — Core language for all ETL, modeling, and orchestration
- **boto3** — AWS SDK for Python, used for S3 data access and management
- **Playwright** — Headless browser automation for scraping London data
- **pandas** — Data manipulation and validation
- **psycopg** — PostgreSQL database driver for Python
- **dbt (Data Build Tool)** — SQL-based data transformation, modeling, and analytics marts
- **PostgreSQL (AWS RDS)** — Cloud-hosted relational database for analytics
- **AWS S3** — Cloud object storage for raw and processed data
- **AWS EC2 (Ubuntu)** — Cloud compute for orchestration and automation
- **Streamlit** — Interactive dashboarding and web app framework
- **Plotly** — Advanced data visualization and charting
- **Streamlit Cloud** — Free public hosting for the dashboard
- **dotenv** — Environment variable management
- **pytest** — Automated testing framework
- **Git & GitHub** — Version control and collaboration
- **ChatGPT & Cursor** — AI-assisted coding, documentation, and ideation

---

## Roadmap & Next Steps

### Additional Data Sources
- **Populations**
  - Validate that the population figures used reflect the total volume of people living within bike infrastructure coverage.
- **Weather**
  - Enrich the database with weather data and utilize it in analytics to provide weather-based insight on bike utilization.
- **Covid**
  - Bring in annotated events data to contextualize anomalies and visualize pandemic impact.

### Data Extraction
- **Enhanced Extraction**
  - Build file indexing that keeps track of available files on web locations and diffs against the S3 bucket.
  - Refactor extraction of files from the web to leverage this indexing.
- **S3 Management**
  - Refactor the flow to include an intermediate process that scans S3 and moves files into the right location based on schema, rather than determining the model and raw landing table at load time.

### Database Load
- **Improved Data Modeling**
  - Fix schemas for better performance and investigate indexing at load.
- **Utilize more efficient processing**
  - Remove pandas from the process, if possible, for greater efficiency.

### Analytics
- **Move date-derived fields upstream**
  - Add fields like `day_type` (weekday/weekend), `is_weekday`, `year`, `month`, etc. to staging models (`stg_*`) to ensure all downstream models receive these fields consistently and avoid logic duplication.
- **Explore new metrics, categorizations, and areas of insight**
  - Station-focused metrics
  - Route-focused insight (distance, heatmapping, common routes, inflow vs outflow)
  - Bike-focused insight (in London using bike_id)
  - Segmentation by day of week
  - Segmentation by weather (bring in external data source)
  - Quantified seasonal effects
  - Quantified Covid effects (bring in external data source)

- Further automate and productionize the pipeline.

---

## Contact

If you are interested in gaining access to the database or have questions about the project, please reach out:

christophertrogers37@gmail.com

## Data Sources & Acknowledgements

Special thanks to:
- [Transport for London (cycling.data.tfl.gov.uk)](https://cycling.data.tfl.gov.uk/) for making London Santander Cycles data publicly available
- [Citi Bike / Lyft](https://citibikenyc.com/system-data) for making NYC Citi Bike data publicly available

**This project demonstrates the design and implementation of a modern, cloud-native analytics stack—from raw data extraction to interactive dashboarding—using open-source tools and best practices.**