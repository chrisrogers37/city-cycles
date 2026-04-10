import useSWR from "swr";
import { apiFetch } from "@/lib/api";
import type { CurrentWeatherResponse, ForecastResponse } from "@/lib/types";

export function useWeather(city: string) {
  return useSWR<CurrentWeatherResponse>(
    `/api/weather/${city}`,
    apiFetch<CurrentWeatherResponse>,
    { refreshInterval: 300_000 },
  );
}

export function useForecast(city: string) {
  return useSWR<ForecastResponse>(
    `/api/weather/${city}/forecast`,
    apiFetch<ForecastResponse>,
    { refreshInterval: 300_000 },
  );
}
