"""Pydantic response models for insights endpoint."""

from typing import Optional

from pydantic import BaseModel


class BikingScoreResponse(BaseModel):
    score: int
    label: str
    color: str


class RecommendationResponse(BaseModel):
    text: str
    severity: str
    metric: str
    value: Optional[float] = None


class ClassifiedConditionsResponse(BaseModel):
    weather_category: str
    temperature_band: str
    wind_category: str
    precipitation_intensity: str


class InsightsResponse(BaseModel):
    biking_score: BikingScoreResponse
    recommendations: list[RecommendationResponse]
    classified: ClassifiedConditionsResponse
