"use client";

import { useSimilarDay } from "@/hooks/useSimilarDay";
import { useCityStore } from "@/store/useCityStore";
import { formatRides, formatPrecipitation, monthName } from "@/lib/format";

const CITY_NAMES: Record<string, string> = { nyc: "NYC", london: "London" };

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
      <div className="glass-card p-6 border-l-3 border-l-[var(--color-neutral)]">
        <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
          Days Like Today
        </h3>
        <p className="text-[var(--color-text-secondary)] text-sm leading-relaxed">
          Weather-ride correlation data is not yet available. The weather data pipeline
          needs to run to generate historical comparisons for days like today.
        </p>
      </div>
    );
  }

  const month = data.month ? monthName(data.month) : "";
  const dayType = data.day_type ?? "day";
  const tempBand = data.temperature_band ?? "";
  const precip = formatPrecipitation(data.precipitation_intensity);
  const cityName = CITY_NAMES[city] ?? city;
  const pct = data.pct_change_vs_overall ?? 0;
  const pctDirection = pct >= 0 ? "above" : "below";
  const pctAbs = Math.abs(pct).toFixed(0);
  const peakRange =
    data.peak_hour_start !== null && data.peak_hour_end !== null
      ? `${data.peak_hour_start}:00–${data.peak_hour_end}:00`
      : null;
  const limitedData = data.sample_days !== null && data.sample_days < 10;

  // Tint: green for above +10%, red for below -10%, neutral otherwise
  const tintClass =
    pct >= 10
      ? "border-l-[var(--color-positive)]"
      : pct <= -10
        ? "border-l-[var(--color-warning)]"
        : "border-l-[var(--color-neutral)]";

  return (
    <div className={`glass-card p-6 border-l-3 ${tintClass}`}>
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
        Days Like Today
      </h3>
      <p className="text-[var(--color-text-secondary)] text-sm leading-relaxed">
        On <span className="text-white font-medium">{tempBand} {month} {dayType}s</span> with{" "}
        <span className="text-white font-medium">{precip}</span>,{" "}
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
          {limitedData && <span className="text-[var(--color-caution)]"> (limited data)</span>}
        </p>
      )}
    </div>
  );
}
