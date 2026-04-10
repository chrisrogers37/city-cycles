"use client";

import { useSimilarDay } from "@/hooks/useSimilarDay";
import { useCityStore } from "@/store/useCityStore";

const CITY_NAMES: Record<string, string> = { nyc: "NYC", london: "London" };
const MONTH_NAMES = [
  "", "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

function formatRides(n: number): string {
  return n >= 1000 ? `${(n / 1000).toFixed(1).replace(/\.0$/, "")}k` : String(Math.round(n));
}

export default function SimilarDayCard() {
  const city = useCityStore((s) => s.city);
  const { data, isLoading } = useSimilarDay(city);

  if (isLoading) {
    return (
      <div className="glass-card p-6 animate-pulse">
        <div className="h-4 bg-white/10 rounded w-3/4 mb-3" />
        <div className="h-4 bg-white/10 rounded w-1/2" />
      </div>
    );
  }

  if (!data || data.avg_daily_rides === null) {
    return (
      <div className="glass-card p-6">
        <p className="text-[var(--color-text-muted)] text-sm">
          Historical comparison data is being prepared.
        </p>
      </div>
    );
  }

  const monthName = data.month ? MONTH_NAMES[data.month] : "";
  const dayType = data.day_type ?? "day";
  const tempBand = data.temperature_band ?? "";
  const cityName = CITY_NAMES[city] ?? city;
  const pctDirection = (data.pct_change_vs_overall ?? 0) >= 0 ? "above" : "below";
  const pctAbs = Math.abs(data.pct_change_vs_overall ?? 0).toFixed(0);
  const peakRange =
    data.peak_hour_start !== null && data.peak_hour_end !== null
      ? `${data.peak_hour_start}:00–${data.peak_hour_end}:00`
      : null;

  return (
    <div className="glass-card p-6">
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
        Days Like Today
      </h3>
      <p className="text-[var(--color-text-secondary)] text-sm leading-relaxed">
        On <span className="text-white font-medium">{tempBand} {monthName} {dayType}s</span>,{" "}
        {cityName} averages{" "}
        <span className="text-white font-medium">{formatRides(data.avg_daily_rides)} rides</span>{" "}
        — {pctAbs}% {pctDirection} typical.
        {peakRange && (
          <>
            {" "}Peak activity: <span className="text-white font-medium">{peakRange}</span>.
          </>
        )}
      </p>
      {data.sample_days !== null && (
        <p className="text-[var(--color-text-muted)] text-xs mt-3">
          Based on {data.sample_days} similar days
        </p>
      )}
    </div>
  );
}
