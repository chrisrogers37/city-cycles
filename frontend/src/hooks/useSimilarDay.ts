import useSWR from "swr";
import { apiFetch } from "@/lib/api";
import type { SimilarDayResponse, SimilarDayHourlyResponse } from "@/lib/types";

export function useSimilarDay(city: string) {
  return useSWR<SimilarDayResponse>(
    `/api/similar-day/${city}`,
    apiFetch<SimilarDayResponse>,
    { refreshInterval: 300_000 },
  );
}

export function useSimilarDayHourly(city: string) {
  return useSWR<SimilarDayHourlyResponse>(
    `/api/similar-day/${city}/hourly`,
    apiFetch<SimilarDayHourlyResponse>,
    { refreshInterval: 300_000 },
  );
}
