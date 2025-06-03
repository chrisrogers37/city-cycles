
# 📥 London Bike Data File Download Specification

This document outlines how to download London Santander Cycle data files from the Transport for London (TfL) open data portal in a programmatic, compliant, and efficient way.

---

## 🔗 Data Source

**Base URL:**
```
https://cycling.data.tfl.gov.uk/usage-stats/
```

This is a public HTTP directory served by TfL, **not** an Amazon S3 bucket. Files are accessible via direct links and are served with directory listing enabled.

---

## 📄 File Naming Pattern

We are interested in files matching the following patterns:

- Contains: `JourneyDataExtract` or `Journey Data Extract`
- Extension: `.csv`
- Date Range: Years **2019 to present**

These are **weekly ride history files** published by TfL.

---

## 📜 License Considerations

TfL uses a modified version of the [Open Government Licence v2.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/).

**You ARE permitted to:**
- Copy, publish, distribute, and adapt the information
- Use it commercially or non-commercially
- Combine it with other datasets

**You MUST:**
- Include attribution:
  ```
  Powered by TfL Open Data
  Contains OS data © Crown copyright and database rights 2016
  Geomni UK Map data © and database rights [2019]
  ```
- Limit traffic to **≤ 500 calls/minute**
- NOT claim endorsement by TfL

**You are NOT permitted to:**
- Use scraping tools on the **Santander Cycles website** (this does not include the data portal)
- Violate the attribution, branding, or traffic limit policies

---

## 🧪 Download Approach

### Required Steps:

1. Fetch the HTML content of `https://cycling.data.tfl.gov.uk/usage-stats/`
2. Parse anchor `<a href="...">` tags to extract file names
3. Match filenames with regex:
   ```
   Journey\s?Data\s?Extract.*\.csv
   ```
4. Filter for filenames where year >= 2019
5. Download files with polite request pacing (e.g., 1 per second)
6. Store each file locally (or in cloud blob storage) for future ingestion

---

## 🔧 Python Example

```python
import requests
from bs4 import BeautifulSoup
import re
import os
from urllib.parse import urljoin
from time import sleep

BASE_URL = "https://cycling.data.tfl.gov.uk/usage-stats/"
DEST_DIR = "./london_data"
os.makedirs(DEST_DIR, exist_ok=True)

response = requests.get(BASE_URL)
soup = BeautifulSoup(response.text, "html.parser")

pattern = re.compile(r"(Journey\s?Data\s?Extract.*\.csv)", re.IGNORECASE)

for link in soup.find_all("a", href=True):
    filename = link['href']
    if pattern.search(filename):
        year_match = re.search(r"(20\d{2})", filename)
        if year_match and int(year_match.group(1)) >= 2019:
            file_url = urljoin(BASE_URL, filename)
            dest_path = os.path.join(DEST_DIR, filename)

            print(f"Downloading {file_url} ...")
            with requests.get(file_url, stream=True) as r:
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            sleep(1)  # respectful delay
```

---

## ✅ Output

Each file will be stored in `./london_data/` with original naming preserved. These CSVs can be parsed using pandas and aligned to a common schema before insertion into the database.

---

## 🧠 Future Enhancements

- Add logging and retry logic
- Implement checksum validation or last-modified caching
- Store files in S3 or GCS instead of local
- Wrap into an Airflow DAG or Prefect flow
