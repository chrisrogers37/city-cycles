"use client";

import { useWeather } from "@/hooks/useWeather";
import { useInsights } from "@/hooks/useInsights";
import { getScoreColor } from "@/lib/colors";
import { useCityStore } from "@/store/useCityStore";

const CITY_LABELS: Record<string, string> = { nyc: "NYC", london: "London" };

function CityWeatherCard({
  city,
}: {
  city: "nyc" | "london";
}) {
  const tempUnit = useCityStore((s) => s.tempUnit);
  const { data: weather, isLoading: wLoading } = useWeather(city);
  const { data: insights, isLoading: iLoading } = useInsights(city);

  const label = CITY_LABELS[city];
  const isLoading = wLoading || iLoading;

  if (isLoading) {
    return (
      <div className="flex-1 p-5 animate-pulse">
        <div className="h-5 bg-white/10 rounded w-20 mb-3" />
        <div className="h-8 bg-white/10 rounded w-32 mb-2" />
        <div className="h-4 bg-white/10 rounded w-24" />
      </div>
    );
  }

  const score = insights?.biking_score?.score;
  const scoreColor = score != null ? getScoreColor(score) : undefined;

  return (
    <div className="flex-1 p-5">
      <p className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-2">
        {label}
      </p>
      {weather ? (
        <>
          <p className="text-2xl font-light text-white">
            {Math.round(tempUnit === "fahrenheit" ? weather.temperature_f : weather.temperature_c)}°{tempUnit === "fahrenheit" ? "F" : "C"}
            <span className="text-base text-[var(--color-text-secondary)] ml-2">
              {weather.weather_description}
            </span>
          </p>
          {score != null && (
            <div className="flex items-center gap-2 mt-2">
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{ background: scoreColor }}
              />
              <span className="text-sm text-[var(--color-text-secondary)]">
                Biking Score: <span className="text-white font-medium">{score}</span>
              </span>
            </div>
          )}
        </>
      ) : (
        <p className="text-sm text-[var(--color-text-muted)]">Weather unavailable</p>
      )}
    </div>
  );
}

export default function DualWeatherHeader() {
  return (
    <div className="glass-card flex flex-col sm:flex-row divide-y sm:divide-y-0 sm:divide-x divide-white/8">
      <CityWeatherCard city="nyc" />
      <CityWeatherCard city="london" />
    </div>
  );
}
