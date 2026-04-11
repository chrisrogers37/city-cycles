import useSWR from "swr";
import { apiFetch } from "@/lib/api";
import type {
  DailyMetricsResponse,
  MonthlyTrendRow,
  DurationTrendRow,
  HourlyPatternRow,
  MemberAnalysisRow,
  StationGrowthRow,
  StationPerformanceRow,
  WeatherCorrelationTempRow,
  WeatherCorrelationPrecipRow,
  WeatherImpactRow,
  HourlyWeatherImpactRow,
} from "@/lib/types";

/** Build a query-string path. Omits null/undefined values. */
function withParams(base: string, params: Record<string, string | number | null | undefined>): string {
  const qs = Object.entries(params)
    .filter(([, v]) => v != null)
    .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
    .join("&");
  return qs ? `${base}?${qs}` : base;
}

const SWR_OPTS = { refreshInterval: 300_000 } as const;

/* ── Date-ranged endpoints (Ride Analytics) ────────────────────────── */

export function useDailyMetrics(city: string, startDate: string, endDate: string) {
  const path = withParams(`/api/analytics/${city}/daily-metrics`, {
    start_date: startDate,
    end_date: endDate,
  });
  return useSWR<DailyMetricsResponse>(path, apiFetch<DailyMetricsResponse>, SWR_OPTS);
}

export function useMonthlyTrends(city: string, startDate: string, endDate: string, aggregation: "avg" | "sum" = "avg") {
  const path = withParams(`/api/analytics/${city}/monthly-trends`, {
    start_date: startDate,
    end_date: endDate,
    aggregation,
  });
  return useSWR<MonthlyTrendRow[]>(path, apiFetch<MonthlyTrendRow[]>, SWR_OPTS);
}

export function useDurationTrends(city: string, startDate: string, endDate: string) {
  const path = withParams(`/api/analytics/${city}/duration-trends`, {
    start_date: startDate,
    end_date: endDate,
  });
  return useSWR<DurationTrendRow[]>(path, apiFetch<DurationTrendRow[]>, SWR_OPTS);
}

export function useMemberAnalysis(city: string, startDate?: string, endDate?: string) {
  const path = withParams(`/api/analytics/${city}/member-analysis`, {
    start_date: startDate,
    end_date: endDate,
  });
  return useSWR<MemberAnalysisRow[]>(city === "nyc" ? path : null, apiFetch<MemberAnalysisRow[]>, SWR_OPTS);
}

/* ── Non-date endpoints ────────────────────────────────────────────── */

export function useHourlyPatterns(city: string) {
  return useSWR<HourlyPatternRow[]>(
    `/api/analytics/${city}/hourly-patterns`,
    apiFetch<HourlyPatternRow[]>,
    SWR_OPTS,
  );
}

export function useStationGrowth(city: string) {
  return useSWR<StationGrowthRow[]>(
    `/api/analytics/${city}/station-growth`,
    apiFetch<StationGrowthRow[]>,
    SWR_OPTS,
  );
}

export function useStationPerformance(
  city: string,
  weatherCondition: string,
  hourStart?: number,
  hourEnd?: number,
  limit?: number,
) {
  const path = withParams(`/api/analytics/${city}/station-performance`, {
    weather_condition: weatherCondition,
    hour_start: hourStart,
    hour_end: hourEnd,
    limit,
  });
  return useSWR<StationPerformanceRow[]>(path, apiFetch<StationPerformanceRow[]>, SWR_OPTS);
}

/* ── Weather correlation & impact ──────────────────────────────────── */

export function useWeatherCorrelationTemp(city: string) {
  return useSWR<WeatherCorrelationTempRow[]>(
    `/api/analytics/${city}/weather-correlation/temperature`,
    apiFetch<WeatherCorrelationTempRow[]>,
    SWR_OPTS,
  );
}

export function useWeatherCorrelationPrecip(city: string) {
  return useSWR<WeatherCorrelationPrecipRow[]>(
    `/api/analytics/${city}/weather-correlation/precipitation`,
    apiFetch<WeatherCorrelationPrecipRow[]>,
    SWR_OPTS,
  );
}

export function useWeatherImpact(city: string) {
  return useSWR<WeatherImpactRow[]>(
    `/api/analytics/${city}/weather-impact`,
    apiFetch<WeatherImpactRow[]>,
    SWR_OPTS,
  );
}

export function useHourlyWeatherImpact(city: string) {
  return useSWR<HourlyWeatherImpactRow[]>(
    `/api/analytics/${city}/weather-impact/hourly`,
    apiFetch<HourlyWeatherImpactRow[]>,
    SWR_OPTS,
  );
}
