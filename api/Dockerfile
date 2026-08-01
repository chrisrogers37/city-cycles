FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy API source (includes moved services: weather_service, recommendation_engine, data_loader)
COPY api/ api/

# Create data directory for parquet files downloaded from S3
RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1

# Trust X-Forwarded-For only from Railway's internal edge peers (RFC 6598 shared
# address space — never a routable public client), so per-IP rate limiting keys on
# the real visitor. A specific CIDR (not '*') makes uvicorn walk the XFF chain
# right-to-left past trusted hops, so client-supplied entries are never believed.
# Override FORWARDED_ALLOW_IPS in the service dashboard if Railway's peer block
# ever drifts (symptom: all clients share one rate-limit bucket again).
CMD uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips "${FORWARDED_ALLOW_IPS:-100.64.0.0/10}"
