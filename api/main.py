"""
City Cycles API — FastAPI application.

Wraps existing Python modules (weather_service, recommendation_engine)
and DuckDB parquet queries behind HTTP endpoints.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from api.routes import weather, insights, similar_day, analytics

logger = logging.getLogger(__name__)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add standard security headers to all responses."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-XSS-Protection"] = "0"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; frame-ancestors 'none'"
        )
        if os.getenv("RAILWAY_ENVIRONMENT"):
            response.headers["Strict-Transport-Security"] = (
                "max-age=63072000; includeSubDomains"
            )
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Download parquet files from S3 on startup if not present locally."""
    from api.services.data_loader import ensure_local_parquet_files

    logger.info("Ensuring local parquet files are available...")
    ensure_local_parquet_files()
    logger.info("Parquet files ready.")
    yield


app = FastAPI(
    title="City Cycles API",
    description="Bike share analytics for NYC and London — weather, insights, and ride patterns.",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — configurable via CORS_ORIGIN env var
origins = os.getenv("CORS_ORIGIN", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Security headers
app.add_middleware(SecurityHeadersMiddleware)

# Mount routers
app.include_router(weather.router)
app.include_router(insights.router)
app.include_router(similar_day.router)
app.include_router(analytics.router)


@app.get("/health")
def health():
    """Health check endpoint."""
    from api.dependencies import DATA_DIR, parquet_exists
    from api.services.data_loader import MARTS

    loaded = sum(1 for m in MARTS if parquet_exists(m))
    return {
        "status": "healthy",
        "marts_loaded": loaded,
        "marts_total": len(MARTS),
        "data_dir": DATA_DIR,
    }
