"""Tests for dashboard/utils/css_injector.py."""

import pytest
import os


class TestWeatherCodeMapping:
    """Test WMO weather code to animation class mapping."""

    def test_clear_code_0(self):
        """WMO code 0 should produce clear animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(0) == 'weather-clear'

    def test_clear_code_1(self):
        """WMO code 1 should produce clear animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(1) == 'weather-clear'

    def test_cloudy_codes(self):
        """WMO codes 2-3 should produce cloudy animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(2) == 'weather-cloudy'
        assert get_animation_class(3) == 'weather-cloudy'

    def test_fog_codes(self):
        """WMO codes 45, 48 should produce cloudy animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(45) == 'weather-cloudy'
        assert get_animation_class(48) == 'weather-cloudy'

    def test_rain_codes(self):
        """WMO codes 51-67 should produce rain animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(51) == 'weather-rain'
        assert get_animation_class(61) == 'weather-rain'
        assert get_animation_class(65) == 'weather-rain'

    def test_snow_codes(self):
        """WMO codes 71-77 should produce snow animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(71) == 'weather-snow'
        assert get_animation_class(75) == 'weather-snow'
        assert get_animation_class(77) == 'weather-snow'

    def test_rain_shower_codes(self):
        """WMO codes 80-82 should produce rain animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(80) == 'weather-rain'
        assert get_animation_class(82) == 'weather-rain'

    def test_snow_shower_codes(self):
        """WMO codes 85-86 should produce snow animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(85) == 'weather-snow'
        assert get_animation_class(86) == 'weather-snow'

    def test_thunderstorm_codes(self):
        """WMO codes 95-99 should produce rain animation."""
        from dashboard.utils.css_injector import get_animation_class
        assert get_animation_class(95) == 'weather-rain'
        assert get_animation_class(99) == 'weather-rain'


class TestParticleHTML:
    """Test weather animation particle HTML generation."""

    def test_rain_has_40_drops(self):
        """Rain animation should generate 40 raindrop divs."""
        from dashboard.utils.css_injector import _build_particle_html
        html = _build_particle_html(61)  # rain
        assert html.count('class="raindrop"') == 40

    def test_snow_has_30_flakes(self):
        """Snow animation should generate 30 snowflake divs."""
        from dashboard.utils.css_injector import _build_particle_html
        html = _build_particle_html(71)  # snow
        assert html.count('class="snowflake"') == 30

    def test_clear_is_single_div(self):
        """Clear animation should be a single div."""
        from dashboard.utils.css_injector import _build_particle_html
        html = _build_particle_html(0)
        assert 'weather-clear' in html

    def test_cloudy_is_single_div(self):
        """Cloudy animation should be a single div."""
        from dashboard.utils.css_injector import _build_particle_html
        html = _build_particle_html(3)
        assert 'weather-cloudy' in html


class TestCSSFileLoading:
    """Test CSS file loading from static directory."""

    def test_static_dir_exists(self):
        """The static directory should exist."""
        from dashboard.utils.css_injector import STATIC_DIR
        assert os.path.isdir(STATIC_DIR)

    def test_weather_animations_css_exists(self):
        """weather_animations.css should exist in static directory."""
        from dashboard.utils.css_injector import STATIC_DIR
        assert os.path.isfile(os.path.join(STATIC_DIR, 'weather_animations.css'))

    def test_atmospheric_theme_css_exists(self):
        """atmospheric_theme.css should exist in static directory."""
        from dashboard.utils.css_injector import STATIC_DIR
        assert os.path.isfile(os.path.join(STATIC_DIR, 'atmospheric_theme.css'))

    def test_load_css_returns_content(self):
        """_load_css_file should return non-empty string."""
        from dashboard.utils.css_injector import _load_css_file
        content = _load_css_file('weather_animations.css')
        assert len(content) > 0
        assert '@keyframes' in content

    def test_load_missing_file_raises(self):
        """_load_css_file should raise for missing files."""
        from dashboard.utils.css_injector import _load_css_file
        with pytest.raises(FileNotFoundError):
            _load_css_file('nonexistent.css')
