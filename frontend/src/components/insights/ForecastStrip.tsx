"use client";

import { useForecast } from "@/hooks/useWeather";
import { useCityStore } from "@/store/useCityStore";
import type { TempUnit } from "@/lib/types";

function formatHour(iso: string, tz: string): string {
  const d = new Date(iso);
  return d.toLocaleString("en-US", {
    timeZone: tz,
    hour: "numeric",
    hour12: true,
  }).toLowerCase().replace(" ", "");
}

function tempValue(c: number, f: number, unit: TempUnit): number {
  return Math.round(unit === "fahrenheit" ? f : c);
}

export default function ForecastStrip() {
  const city = useCityStore((s) => s.city);
  const tempUnit = useCityStore((s) => s.tempUnit);
  const { data, isLoading } = useForecast(city);
  const tz = city === "london" ? "Europe/London" : "America/New_York";

  if (isLoading) {
    return (
      <div className="glass-card p-4">
        <div className="flex gap-4 overflow-x-auto">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="shrink-0 w-14 h-20 bg-white/5 rounded animate-pulse" />
          ))}
        </div>
      </div>
    );
  }

  if (!data || data.hours.length === 0) return null;

  return (
    <div className="glass-card p-4">
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-3">
        24-Hour Forecast
      </h3>
      <div className="flex gap-3 overflow-x-auto pb-2 snap-x snap-mandatory scrollbar-hide">
        {data.hours.map((hour, i) => (
          <div
            key={i}
            className="shrink-0 flex flex-col items-center gap-1.5 snap-start min-w-[3.5rem]"
          >
            <span className="text-[10px] text-[var(--color-text-muted)]">
              {formatHour(hour.time, tz)}
            </span>
            <span className="text-base">{hour.weather_emoji}</span>
            <span className="text-xs text-[var(--color-text-secondary)] font-medium">
              {tempValue(hour.temperature_c, hour.temperature_f, tempUnit)}°
            </span>
            {hour.precipitation_mm > 0 && (
              <div
                className="w-1.5 h-1.5 rounded-full"
                style={{ background: "var(--color-neutral)" }}
                title={`${hour.precipitation_mm.toFixed(1)}mm`}
              />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
