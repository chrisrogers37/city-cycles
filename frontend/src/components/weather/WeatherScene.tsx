"use client";

import SkyGradient from "./SkyGradient";
import WeatherHero from "./WeatherHero";
import WeatherNarrative from "./WeatherNarrative";
import CitySilhouette from "./CitySilhouette";
import FogOverlay from "./FogOverlay";
import CloudOverlay from "./CloudOverlay";
import SunOverlay from "./SunOverlay";
import WeatherCanvas from "./WeatherCanvas";
import { useWeather } from "@/hooks/useWeather";
import { useInsights } from "@/hooks/useInsights";
import { useSimilarDay } from "@/hooks/useSimilarDay";
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
  const { data: insights } = useInsights(city);
  const { data: similarDay, isLoading: similarDayLoading } = useSimilarDay(city);

  const category = weather?.weather_category as WeatherCategory | undefined;

  return (
    <SkyGradient period={period} weatherCategory={category}>
      <WeatherEffect category={category} />

      {/* Hero text + biking score */}
      <div className="relative min-h-screen flex flex-col">
        <WeatherHero weather={weather} isLoading={weatherLoading} />

        <div className="flex-1" />

        <div className="flex justify-center pb-[26vh] relative z-20">
          <WeatherNarrative
            weather={weather}
            similarDay={similarDay}
            recommendations={insights?.recommendations}
            isLoading={weatherLoading || similarDayLoading}
          />
        </div>

        <div className="absolute bottom-8 left-1/2 -translate-x-1/2 flex flex-col items-center gap-1 animate-bounce z-20">
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
