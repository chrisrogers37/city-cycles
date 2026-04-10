"""Insights API route — biking score, classified conditions, ranked recommendations."""

import logging

from fastapi import APIRouter, HTTPException

from api.dependencies import CityParam
from api.models.insights import (
    BikingScoreResponse,
    ClassifiedConditionsResponse,
    InsightsResponse,
    RecommendationResponse,
)
from api.services.weather_bridge import get_city_weather, weather_to_conditions
from api import cache
from dashboard.recommendation_engine import get_recommendations
from dashboard.weather_service import WeatherAPIError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/insights", tags=["insights"])

INSIGHTS_TTL = 900  # 15 minutes


@router.get("/{city}", response_model=InsightsResponse)
def get_insights(city: CityParam):
    """Full recommendation result — biking score, conditions, and insights."""
    cache_key = f"insights:{city.value}"
    cached = cache.get(cache_key, INSIGHTS_TTL)
    if cached is not None:
        return cached

    try:
        weather = get_city_weather(city.value)
    except WeatherAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    conditions = weather_to_conditions(weather)
    result = get_recommendations(conditions)

    response = InsightsResponse(
        biking_score=BikingScoreResponse(
            score=result.biking_score.score,
            label=result.biking_score.label,
            color=result.biking_score.color,
        ),
        recommendations=[
            RecommendationResponse(
                text=r.text,
                severity=r.severity.value,
                metric=r.metric,
                value=r.value,
            )
            for r in result.recommendations
        ],
        classified=ClassifiedConditionsResponse(
            weather_category=result.classified.weather_category.value,
            temperature_band=result.classified.temperature_band.value,
            wind_category=result.classified.wind_category.value,
            precipitation_intensity=result.classified.precipitation_intensity.value,
        ),
    )

    cache.set(cache_key, response)
    return response
