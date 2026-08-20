"""Tests for the API's transport-security middleware stack.

These assert *behaviour at the edge* — security headers, CORS, and per-IP rate
limiting — rather than the wiring in api.main. The distinction matters: the
middleware is supplied by third-party packages (starlette, slowapi) and selected
by version pins, so the wiring can be untouched and correct while the behaviour
it buys silently disappears under a dependency upgrade. Only a request/response
assertion catches that.
"""

import pytest

from tests.conftest_api import *  # noqa: F401,F403


EXPECTED_SECURITY_HEADERS = {
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "strict-origin-when-cross-origin",
    "x-xss-protection": "0",
    "content-security-policy": "default-src 'none'; frame-ancestors 'none'",
}

# Cheap, always routable, and not exempt from the limiter.
LIMITED_PATH = "/api/insights/nyc"

# api.main's fallback when CORS_ORIGIN is unset, which is how tests run.
DEFAULT_ALLOWED_ORIGIN = "http://localhost:3000"


class TestSecurityHeaders:
    def test_all_security_headers_present(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        actual = {k: response.headers.get(k) for k in EXPECTED_SECURITY_HEADERS}
        assert actual == EXPECTED_SECURITY_HEADERS

    def test_security_headers_present_on_api_routes(self, client):
        response = client.get(LIMITED_PATH)
        for header, value in EXPECTED_SECURITY_HEADERS.items():
            assert response.headers.get(header) == value


class TestCORS:
    def test_allowed_origin_is_echoed(self, client):
        response = client.get("/health", headers={"Origin": DEFAULT_ALLOWED_ORIGIN})
        assert response.headers.get("access-control-allow-origin") == DEFAULT_ALLOWED_ORIGIN

    def test_disallowed_origin_is_not_echoed(self, client):
        response = client.get(
            "/health", headers={"Origin": "https://not-an-allowed-origin.example"}
        )
        assert response.headers.get("access-control-allow-origin") is None


class TestRateLimiting:
    """Per-IP throttling must actually return 429.

    The limit is read off the response headers rather than hardcoded, so the
    test tracks the RATE_LIMIT setting instead of drifting from it.
    """

    def test_rate_limit_headers_are_published(self, client):
        response = client.get(LIMITED_PATH)
        assert response.headers.get("x-ratelimit-limit") is not None
        assert response.headers.get("x-ratelimit-remaining") is not None

    def test_requests_beyond_the_limit_are_rejected(self, client):
        first = client.get(LIMITED_PATH)
        limit = first.headers.get("x-ratelimit-limit")
        if limit is None:
            pytest.fail("No x-ratelimit-limit header — the limiter is not running.")

        codes = [client.get(LIMITED_PATH).status_code for _ in range(int(limit) + 1)]
        assert 429 in codes, (
            f"Sent {len(codes) + 1} requests against a limit of {limit} and never got "
            f"a 429 — rate limiting is not being enforced. Codes seen: {set(codes)}"
        )

    def test_health_endpoint_is_exempt(self, client):
        # Burst past the configured limit; a missing header means the limiter is
        # down, which the two tests above already report — fall back to a generous
        # count here rather than adding a third alarm for the same cause.
        limit = client.get(LIMITED_PATH).headers.get("x-ratelimit-limit")
        burst = int(limit) + 10 if limit else 100
        codes = [client.get("/health").status_code for _ in range(burst)]
        assert set(codes) == {200}
