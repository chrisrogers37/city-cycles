import useSWR from "swr";
import { apiFetch } from "@/lib/api";
import type { InsightsResponse } from "@/lib/types";

export function useInsights(city: string) {
  return useSWR<InsightsResponse>(
    `/api/insights/${city}`,
    apiFetch<InsightsResponse>,
    { refreshInterval: 300_000 },
  );
}
