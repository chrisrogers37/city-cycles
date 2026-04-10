"""
Simple in-memory TTL cache for API responses.

Replaces Streamlit's @st.cache_data with a lightweight dict-based cache.
"""

import time
from typing import Any, Optional

_cache: dict[str, tuple[float, Any]] = {}


def get(key: str, ttl_seconds: int) -> Optional[Any]:
    """Return cached value if within TTL, else None."""
    if key in _cache:
        stored_at, value = _cache[key]
        if time.time() - stored_at < ttl_seconds:
            return value
        del _cache[key]
    return None


def set(key: str, value: Any) -> None:
    """Store a value in the cache with the current timestamp."""
    _cache[key] = (time.time(), value)


def clear() -> None:
    """Clear all cached entries."""
    _cache.clear()
