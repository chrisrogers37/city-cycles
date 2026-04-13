/** TypeScript interfaces matching FastAPI response models (api/models/). */

export type City = "nyc" | "london";
export type TempUnit = "fahrenheit" | "celsius";

export interface CurrentWeatherResponse {
  city: string;
  time: string;
  temperature_c: number;
  temperature_f: number;
  apparent_temperature: number;
  humidity: number;
  precipitation_mm: number;
  rain_mm: number;
  snowfall_cm: number;
  weather_code: number;
  weather_description: string;
  weather_category: string;
  weather_emoji: string;
  cloud_cover: number;
  wind_speed_kmh: number;
  wind_gusts_kmh: number;
}

export interface HourlyForecastResponse {
  time: string;
  temperature_c: number;
  temperature_f: number;
  apparent_temperature: number;
  humidity: number;
  precipitation_probability: number;
  precipitation_mm: number;
  weather_code: number;
  weather_description: string;
  weather_category: string;
  weather_emoji: string;
  wind_speed_kmh: number;
}

export interface ForecastResponse {
  city: string;
  hours: HourlyForecastResponse[];
}

export interface BikingScoreResponse {
  score: number;
  label: string;
  color: string;
}

export interface RecommendationResponse {
  text: string;
  severity: string;
  metric: string;
  value: number | null;
}

export interface ClassifiedConditionsResponse {
  weather_category: string;
  temperature_band: string;
  wind_category: string;
  precipitation_intensity: string;
}

export interface InsightsResponse {
  biking_score: BikingScoreResponse;
  recommendations: RecommendationResponse[];
  classified: ClassifiedConditionsResponse;
}

export interface SimilarDayResponse {
  avg_daily_rides: number | null;
  pct_change_vs_overall: number | null;
  avg_duration_minutes: number | null;
  duration_pct_change_vs_overall: number | null;
  peak_hour_start: number | null;
  peak_hour_end: number | null;
  sample_days: number | null;
  month: number | null;
  day_type: string | null;
  temperature_band: string | null;
  precipitation_intensity: string | null;
}

export interface HourlyBucketResponse {
  hour_of_day: number;
  similar_day_avg_rides: number;
  overall_avg_rides: number | null;
  similar_day_avg_duration: number | null;
  member_rides: number | null;
  casual_rides: number | null;
  sample_days: number | null;
}

export interface ClassificationResponse {
  temperature_band: string;
  precipitation_intensity: string;
  month_num: number;
  day_type: string;
}

export interface SimilarDayHourlyResponse {
  hours: HourlyBucketResponse[];
  classification: ClassificationResponse;
}

export type TimePeriod = "night" | "dawn" | "morning" | "day" | "golden" | "dusk";

export type WeatherCategory =
  | "clear"
  | "cloudy"
  | "fog"
  | "drizzle"
  | "rain"
  | "snow"
  | "thunderstorm";

export type Severity = "positive" | "neutral" | "caution" | "warning";

/* ── Analytics endpoint response types ─────────────────────────────── */

export interface DailyMetricsResponse {
  total_rides: number;
  avg_daily_rides: number;
  avg_ride_duration_minutes: number | null;
}

export interface MonthlyTrendRow {
  month: number;
  year: number;
  metric_value: number;
}

export interface DurationTrendRow {
  month: number;
  year: number;
  avg_duration: number | null;
}

export interface HourlyPatternRow {
  hour_of_day: number;
  ride_count: number;
}

export interface WeatherCorrelationTempRow {
  temp_range: string;
  avg_rides: number;
  days_observed: number;
}

export interface WeatherCorrelationPrecipRow {
  precip_category: string;
  avg_rides: number;
  days_observed: number;
}

export interface WeatherImpactRow {
  weather_condition: string;
  pct_change: number;
  avg_rides: number;
  total_observations: number;
}

export interface HourlyWeatherImpactRow {
  hour_of_day: number;
  rain_impact: number | null;
  snow_impact: number | null;
  fog_impact: number | null;
}

export interface StationPerformanceRow {
  station_id: string;
  station_name: string;
  latitude: number | null;
  longitude: number | null;
  avg_pct_change: number;
  total_rides_in_condition: number;
  avg_duration: number;
}

export interface MemberAnalysisRow {
  month: string;
  member_percentage: number;
}

export interface StationGrowthRow {
  year: number;
  station_count: number;
}
