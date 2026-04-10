"""Tests for the API cache module."""

import time

from api import cache


class TestCache:
    def setup_method(self):
        cache.clear()

    def test_set_and_get(self):
        cache.set("key1", "value1")
        assert cache.get("key1", ttl_seconds=60) == "value1"

    def test_expired_returns_none(self):
        cache.set("key2", "value2")
        # Expired after 0 seconds
        assert cache.get("key2", ttl_seconds=0) is None

    def test_missing_key_returns_none(self):
        assert cache.get("nonexistent", ttl_seconds=60) is None

    def test_clear(self):
        cache.set("key3", "value3")
        cache.clear()
        assert cache.get("key3", ttl_seconds=60) is None

    def test_overwrite(self):
        cache.set("key4", "old")
        cache.set("key4", "new")
        assert cache.get("key4", ttl_seconds=60) == "new"
