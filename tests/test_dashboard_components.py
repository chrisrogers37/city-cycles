"""Tests for dashboard component modules (non-Streamlit logic only)."""

import pytest


class TestBikingScoreGauge:
    """Test the biking score gauge component."""

    def test_score_color_high(self):
        """Score >= 80 should return green."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(85) == '#2ECC71'
        assert _score_color(100) == '#2ECC71'

    def test_score_color_medium_high(self):
        """Score 60-79 should return amber."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(65) == '#F39C12'
        assert _score_color(79) == '#F39C12'

    def test_score_color_medium_low(self):
        """Score 40-59 should return orange."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(45) == '#E67E22'
        assert _score_color(59) == '#E67E22'

    def test_score_color_low(self):
        """Score < 40 should return red."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(30) == '#E74C3C'
        assert _score_color(0) == '#E74C3C'

    def test_score_boundary_80(self):
        """Score exactly 80 should be green."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(80) == '#2ECC71'

    def test_score_boundary_60(self):
        """Score exactly 60 should be amber."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(60) == '#F39C12'

    def test_score_boundary_40(self):
        """Score exactly 40 should be orange."""
        from dashboard.components.biking_score_gauge import _score_color
        assert _score_color(40) == '#E67E22'


class TestCityToggle:
    """Test city toggle configuration."""

    def test_city_config_has_both_cities(self):
        """CITY_CONFIG should contain 'nyc' and 'london'."""
        from dashboard.components.city_toggle import CITY_CONFIG
        assert 'nyc' in CITY_CONFIG
        assert 'london' in CITY_CONFIG

    def test_city_config_has_required_fields(self):
        """Each city config should have label, emoji, and timezone."""
        from dashboard.components.city_toggle import CITY_CONFIG
        for city_key, config in CITY_CONFIG.items():
            assert 'label' in config
            assert 'emoji' in config
            assert 'timezone' in config

    def test_nyc_timezone(self):
        """NYC should use America/New_York timezone."""
        from dashboard.components.city_toggle import CITY_CONFIG
        assert CITY_CONFIG['nyc']['timezone'] == 'America/New_York'

    def test_london_timezone(self):
        """London should use Europe/London timezone."""
        from dashboard.components.city_toggle import CITY_CONFIG
        assert CITY_CONFIG['london']['timezone'] == 'Europe/London'


class TestRecommendationCards:
    """Test recommendation card rendering config."""

    def test_severity_emoji_has_all_values(self):
        """SEVERITY_EMOJI should have entries for all Severity enum values."""
        from dashboard.components.recommendation_cards import _SEVERITY_EMOJI
        from dashboard.recommendation_engine import Severity
        for severity in Severity:
            assert severity in _SEVERITY_EMOJI

    def test_severity_bg_color_has_all_values(self):
        """SEVERITY_BG_COLOR should have entries for all Severity enum values."""
        from dashboard.components.recommendation_cards import _SEVERITY_BG_COLOR
        from dashboard.recommendation_engine import Severity
        for severity in Severity:
            assert severity in _SEVERITY_BG_COLOR
