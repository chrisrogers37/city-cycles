FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline source modules (not venv, data, tests, docs)
COPY orchestrator/ orchestrator/
COPY extraction/ extraction/
COPY extracted_file_manager/ extracted_file_manager/
COPY data_models/ data_models/
COPY db_duckdb/ db_duckdb/
COPY dbt_city_cycles/ dbt_city_cycles/
COPY dashboard/ dashboard/
COPY streamlit_data_manager/ streamlit_data_manager/
COPY scripts/ scripts/

# Create dbt profiles.yml with absolute path for container
RUN mkdir -p /root/.dbt && \
    printf '%s\n' \
    "city_cycles:" \
    "  target: prod" \
    "  outputs:" \
    "    prod:" \
    "      type: duckdb" \
    "      path: /app/data/city_cycles.duckdb" \
    "      threads: 2" \
    > /root/.dbt/profiles.yml

# Create data directory for ephemeral DuckDB
RUN mkdir -p /app/data

# Container-appropriate defaults
ENV PYTHONUNBUFFERED=1
ENV DUCKDB_MEMORY_LIMIT=2GB
ENV DUCKDB_THREADS=2

# Make entrypoint executable
RUN chmod +x scripts/railway_entrypoint.sh

ENTRYPOINT ["scripts/railway_entrypoint.sh"]
