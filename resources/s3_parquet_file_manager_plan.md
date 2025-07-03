# 📦 S3 Parquet File Manager – Project Plan

This project provides a modular, automatable file manager for transforming raw ZIP archives stored in S3 into clean Parquet files. These Parquet files will serve as the source of truth for downstream analytical workflows using DuckDB, dbt, or Athena.

---

## 📁 Folder Structure in S3

```
s3://your-bucket-name/
├── raw_zips/              # Raw downloaded ZIPs from web scraper
├── raw_csvs/              # Extracted CSVs from zip files
└── parquet/               # Final clean parquet files (1:1 with CSVs)
```

---

## 🧱 Modular Components

### 1. `unzip_s3_files.py` – ZIP to CSV (S3 → S3, in-memory)

**Goal:** Unzip all `.zip` files from `raw_zips/`, extract `.csv` files, and write them to `raw_csvs/`.

**Steps:**
- List all `.zip` objects in `raw_zips/`
- Download each `.zip` file into memory (`BytesIO`)
- Extract `.csv` files using `zipfile`
- Stream each `.csv` to `raw_csvs/` using `s3.upload_fileobj`

**Outputs:** CSVs in `raw_csvs/` folder.

---

### 2. `csv_to_parquet.py` – CSV to Parquet (S3 → S3, in-memory)

**Goal:** Convert `.csv` files from `raw_csvs/` to `.parquet`, write to `parquet/`.

**Steps:**
- List all `.csv` files in `raw_csvs/`
- Read each `.csv` file directly from S3 into pandas
- Convert DataFrame to `pyarrow.Table`
- Write `parquet` file to memory (`BytesIO`)
- Upload to `parquet/` prefix using `s3.upload_fileobj`

**Outputs:** Final `.parquet` files in `parquet/`.

---

### 3. (Optional) `file_index.json`

**Goal:** Keep track of processed files to avoid reprocessing.

**Steps:**
- Log already-converted files into a local or S3-based `file_index.json`
- On future runs, diff the list of current files against the index

**Bonus:** Can be extended to include schema versioning and data quality tags.

---

## 🧰 Tech Stack

- **boto3** – AWS S3 access and file uploads
- **zipfile** – ZIP file processing
- **pandas** – CSV loading
- **pyarrow** – Parquet conversion
- **io.BytesIO** – In-memory file operations

---

## ⚙️ Suggested Commands

```bash
# Step 1: Unzip all S3 files
python unzip_s3_files.py

# Step 2: Convert all CSVs to Parquet
python csv_to_parquet.py
```

---

## 🚀 Next Steps After Parquet Creation

Once the Parquet layer is complete:

### Option A: Load with DuckDB
```python
import duckdb
df = duckdb.query("SELECT * FROM 's3://your-bucket/parquet/rides.parquet'").to_df()
```

### Option B: Register as `source` in dbt-duckdb

### Option C: Query with AWS Athena
- Configure Glue Data Catalog
- Create external tables over the `parquet/` folder

---

## 📅 Roadmap

| Task | Status |
|------|--------|
| ✅ Unzip ZIPs to CSV (S3-native) | Planned |
| ✅ Convert CSVs to Parquet (S3-native) | Planned |
| 🔜 Add indexing to avoid reprocessing | Optional |
| 🔜 Add data profiling / schema validation | Optional |
| 🔜 Add basic CLI or orchestration logic | Optional |

---

## 👨‍💻 Author
Chris Rogers  
Email: christophertrogers37@gmail.com
