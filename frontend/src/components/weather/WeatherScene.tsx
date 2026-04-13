"use client";

import SkyGradient from "./SkyGradient";
import WeatherHero from "./WeatherHero";
import BikingScore from "./BikingScore";
import CitySilhouette from "./CitySilhouette";
import FogOverlay from "./FogOverlay";
import CloudOverlay from "./CloudOverlay";
import SunOverlay from "./SunOverlay";
import WeatherCanvas from "./WeatherCanvas";
import { useWeather } from "@/hooks/useWeather";
import { useInsights } from "@/hooks/useInsights";
import { useTimePeriod } from "@/hooks/useTimePeriod";
import { useCityStore } from "@/store/useCityStore";
import { needsCanvas } from "@/lib/particles";
import type { WeatherCategory } from "@/lib/types";

function WeatherEffect({ category }: { category?: WeatherCategory }) {
  if (!category) return null;

  // Canvas particles for rain, drizzle, snow, thunderstorm
  if (needsCanvas(category)) {
    return (
      <>
        {category !== "snow" && <CloudOverlay heavy />}
        <WeatherCanvas category={category} />
      </>
    );
  }

  switch (category) {
    case "fog":
      return <FogOverlay />;
    case "cloudy":
      return <CloudOverlay />;
    case "clear":
      return <SunOverlay />;
    default:
      return null;
  }
}

export default function WeatherScene() {
  const city = useCityStore((s) => s.city);
  const period = useTimePeriod(city);
  const { data: weather, isLoading: weatherLoading } = useWeather(city);
  const { data: insights, isLoading: insightsLoading } = useInsights(city);

  const category = weather?.weather_category as WeatherCategory | undefined;

  return (
    <SkyGradient period={period} weatherCategory={category}>
      <WeatherEffect category={category} />

      {/* Hero text + biking score */}
      <div className="relative min-h-screen flex flex-col">
        <WeatherHero weather={weather} isLoading={weatherLoading} />

        <div className="flex-1" />

        {/* Biking score positioned above silhouette */}
        <div className="flex justify-center pb-[18vh]">
          <div className="flex flex-col items-center">
            <BikingScore
              score={insights?.biking_score}
              isLoading={insightsLoading}
            />
            <p className="text-[10px] text-white/30 mt-1 tracking-wide">
              How ideal conditions are for cycling right now
            </p>
            <p className="text-xs text-white/40 mt-4 tracking-wide">
              Based on <span className="text-white/60 font-medium">238M+</span> bike rides across NYC &amp; London
            </p>
          </div>
        </div>

        {/* Scroll indicator */}
        <div className="absolute bottom-8 left-1/2 -translate-x-1/2 flex flex-col items-center gap-1 animate-bounce z-10">
          <span className="text-[10px] text-white/30 uppercase tracking-widest">Scroll</span>
          <svg width="16" height="10" viewBox="0 0 16 10" fill="none" className="text-white/30">
            <path d="M1 1L8 8L15 1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </div>

        <CitySilhouette city={city} />
      </div>
    </SkyGradient>
  );
}
