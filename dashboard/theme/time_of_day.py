"""
Calculate time-of-day period based on city's local timezone.
NYC = America/New_York, London = Europe/London.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

CITY_TIMEZONES = {
    'nyc': ZoneInfo('America/New_York'),
    'london': ZoneInfo('Europe/London'),
}

TIME_PERIODS = [
    # (start_hour, end_hour, period_name)
    (0, 5, 'night'),
    (5, 7, 'dawn'),
    (7, 10, 'morning'),
    (10, 16, 'day'),
    (16, 19, 'golden'),
    (19, 22, 'dusk'),
    (22, 24, 'night'),
]


def get_time_period(city: str) -> str:
    """Return the time-of-day period name for the given city's current local time."""
    tz = CITY_TIMEZONES.get(city, ZoneInfo('UTC'))
    local_hour = datetime.now(tz).hour
    for start, end, period in TIME_PERIODS:
        if start <= local_hour < end:
            return period
    return 'night'
