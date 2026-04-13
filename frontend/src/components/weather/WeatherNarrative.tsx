"use client";

import { useEffect, useState } from "react";
import type { CurrentWeatherResponse, SimilarDayResponse, RecommendationResponse, TempUnit } from "@/lib/types";
import { formatHour12 } from "@/lib/format";
import { useCityStore } from "@/store/useCityStore";

const CITY_NAMES: Record<string, string> = {
  nyc: "New York City",
  london: "London",
};

interface Props {
  weather: CurrentWeatherResponse | undefined;
  similarDay: SimilarDayResponse | undefined;
  recommendations: RecommendationResponse[] | undefined;
  isLoading: boolean;
}

function buildNarrative(
  weather: CurrentWeatherResponse,
  similarDay: SimilarDayResponse | undefined,
  tempUnit: TempUnit,
): string[] {
  const cityName = CITY_NAMES[weather.city] ?? weather.city;
  const desc = weather.weather_description.toLowerCase();
  const lines: string[] = [];

  // Opening line — weather description
  const temp = Math.round(tempUnit === "fahrenheit" ? weather.temperature_f : weather.temperature_c);
  const unit = tempUnit === "fahrenheit" ? "F" : "C";
  lines.push(
    `It's ${desc} and ${temp}°${unit} in ${cityName} right now.`,
  );

  // Duration insight
  // ±5% threshold for duration qualifiers (vs ±3% for volume below) —
  // duration has higher natural variance so we use a wider band to avoid
  // noisy "longer/shorter" labels on unremarkable days.
  if (similarDay?.avg_duration_minutes != null) {
    const dur = Math.round(similarDay.avg_duration_minutes);
    const qualifier =
      similarDay.duration_pct_change_vs_overall != null
        ? similarDay.duration_pct_change_vs_overall > 5
          ? " — longer than usual"
          : similarDay.duration_pct_change_vs_overall < -5
            ? " — shorter than usual"
            : ""
        : "";
    lines.push(
      `On days like this, bike rides tend to last around ${dur} minutes${qualifier}.`,
    );
  }

  // Volume + peak hours
  if (similarDay?.avg_daily_rides != null) {
    const rides = Math.round(similarDay.avg_daily_rides).toLocaleString("en-US");
    const pctDir =
      similarDay.pct_change_vs_overall != null
        ? Math.abs(similarDay.pct_change_vs_overall) >= 3
          ? similarDay.pct_change_vs_overall > 0
            ? `, about ${Math.abs(Math.round(similarDay.pct_change_vs_overall))}% more than average`
            : `, about ${Math.abs(Math.round(similarDay.pct_change_vs_overall))}% fewer than average`
          : ""
        : "";

    let peakInfo = "";
    if (similarDay.peak_hour_start != null && similarDay.peak_hour_end != null) {
      peakInfo = ` Peak activity hits between ${formatHour12(similarDay.peak_hour_start)} and ${formatHour12(similarDay.peak_hour_end)}.`;
    }

    lines.push(
      `The city averages ${rides} rides in similar weather${pctDir}.${peakInfo}`,
    );
  }

  return lines;
}

function pickTagline(recommendations: RecommendationResponse[] | undefined): string | null {
  if (!recommendations?.length) return null;
  // Find a positive or caution recommendation for a snappy closing line
  const positive = recommendations.find((r) => r.severity === "positive");
  if (positive) return positive.text;
  const caution = recommendations.find((r) => r.severity === "caution");
  if (caution) return caution.text;
  return null;
}

export default function WeatherNarrative({
  weather,
  similarDay,
  recommendations,
  isLoading,
}: Props) {
  const tempUnit = useCityStore((s) => s.tempUnit);
  const [fadeIn, setFadeIn] = useState(false);

  const ready = !isLoading && !!weather;

  useEffect(() => {
    if (!ready) {
      // Reset handled via cleanup — no setState in effect body
      return;
    }
    const id = setTimeout(() => setFadeIn(true), 100);
    return () => {
      clearTimeout(id);
      setFadeIn(false);
    };
  }, [ready]);

  const visible = ready && fadeIn;

  if (isLoading || !weather) {
    return (
      <div className="flex flex-col items-center max-w-md mx-auto">
        <div className="space-y-3 w-full">
          <div className="h-4 bg-white/5 rounded animate-pulse w-4/5 mx-auto" />
          <div className="h-4 bg-white/5 rounded animate-pulse w-3/5 mx-auto" />
          <div className="h-4 bg-white/5 rounded animate-pulse w-2/3 mx-auto" />
        </div>
      </div>
    );
  }

  const lines = buildNarrative(weather, similarDay, tempUnit);
  const tagline = pickTagline(recommendations);

  return (
    <div
      className={`flex flex-col items-center max-w-lg mx-auto px-6 transition-all duration-700 ${
        visible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-4"
      }`}
    >
      <p
        className="text-center text-base/relaxed text-white/75 font-light"
        style={{ textShadow: "0 1px 12px rgba(0,0,0,0.4)" }}
      >
        {lines.map((line, i) => (
          <span key={i}>
            {i > 0 && " "}
            {line}
          </span>
        ))}
      </p>

      {tagline && (
        <p
          className="text-sm text-white/45 mt-3 italic"
          style={{ textShadow: "0 1px 8px rgba(0,0,0,0.3)" }}
        >
          {tagline}
        </p>
      )}

      <p
        className="text-[11px] text-white/30 mt-5 tracking-wide"
        style={{ textShadow: "0 1px 6px rgba(0,0,0,0.3)" }}
      >
        Powered by <span className="text-white/50 font-medium">238M+</span> bike rides
      </p>
    </div>
  );
}
