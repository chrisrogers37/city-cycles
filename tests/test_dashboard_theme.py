"""Tests for dashboard/theme/time_of_day.py and plotly_template.py."""

import pytest
from unittest.mock import patch
from datetime import datetime
from zoneinfo import ZoneInfo


class TestTimePeriod:
    """Test time-of-day period calculation."""

    def _get_period_at_hour(self, hour: int, city: str = 'nyc') -> str:
        """Helper: get time period for a given hour."""
        from dashboard.theme.time_of_day import get_time_period, CITY_TIMEZONES
        tz = CITY_TIMEZONES.get(city, ZoneInfo('UTC'))
        mock_dt = datetime(2026, 2, 12, hour, 0, tzinfo=tz)
        with patch('dashboard.theme.time_of_day.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_dt
            return get_time_period(city)

    def test_night_early_morning(self):
        """Hours 0-4 should return 'night'."""
        assert self._get_period_at_hour(2) == 'night'

    def test_night_late_evening(self):
        """Hours 22-23 should return 'night'."""
        assert self._get_period_at_hour(23) == 'night'

    def test_dawn_period(self):
        """Hours 5-6 should return 'dawn'."""
        assert self._get_period_at_hour(6) == 'dawn'

    def test_morning_period(self):
        """Hours 7-9 should return 'morning'."""
        assert self._get_period_at_hour(8) == 'morning'

    def test_day_period(self):
        """Hours 10-15 should return 'day'."""
        assert self._get_period_at_hour(12) == 'day'

    def test_golden_hour_period(self):
        """Hours 16-18 should return 'golden'."""
        assert self._get_period_at_hour(17) == 'golden'

    def test_dusk_period(self):
        """Hours 19-21 should return 'dusk'."""
        assert self._get_period_at_hour(20) == 'dusk'

    def test_london_uses_london_timezone(self):
        """London should use Europe/London timezone."""
        from dashboard.theme.time_of_day import CITY_TIMEZONES
        assert CITY_TIMEZONES['london'] == ZoneInfo('Europe/London')

    def test_unknown_city_uses_utc(self):
        """Unknown city should fall back to UTC."""
        from dashboard.theme.time_of_day import get_time_period, CITY_TIMEZONES
        tz = ZoneInfo('UTC')
        mock_dt = datetime(2026, 2, 12, 14, 0, tzinfo=tz)
        with patch('dashboard.theme.time_of_day.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_dt
            result = get_time_period('tokyo')
        assert result == 'day'

    def test_boundary_hour_5_is_dawn(self):
        """Hour 5 exactly should be 'dawn' not 'night'."""
        assert self._get_period_at_hour(5) == 'dawn'

    def test_boundary_hour_22_is_night(self):
        """Hour 22 exactly should be 'night' not 'dusk'."""
        assert self._get_period_at_hour(22) == 'night'


class TestAtmosphericTemplate:
    """Test the custom Plotly template."""

    def test_template_has_transparent_background(self):
        """Template should have transparent paper and plot backgrounds."""
        from dashboard.theme.plotly_template import create_atmospheric_template
        template = create_atmospheric_template()
        assert template.layout.paper_bgcolor == 'rgba(0, 0, 0, 0)'
        assert template.layout.plot_bgcolor == 'rgba(0, 0, 0, 0)'

    def test_template_has_color_sequence(self):
        """Template should define a colorway with at least 5 colors."""
        from dashboard.theme.plotly_template import create_atmospheric_template
        template = create_atmospheric_template()
        assert len(template.layout.colorway) >= 5

    def test_register_template(self):
        """register_template should add 'atmospheric' to Plotly registry."""
        from dashboard.theme.plotly_template import register_template
        import plotly.io as pio
        register_template()
        assert 'atmospheric' in pio.templates

    def test_atmospheric_colors_are_hex(self):
        """All colors in ATMOSPHERIC_COLORS should be valid hex."""
        from dashboard.theme.plotly_template import ATMOSPHERIC_COLORS
        for color in ATMOSPHERIC_COLORS:
            assert color.startswith('#')
            assert len(color) == 7
