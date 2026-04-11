"use client";

import SkyGradient from "./SkyGradient";
import WeatherHero from "./WeatherHero";
import BikingScore from "./BikingScore";
import CitySilhouette from "./CitySilhouette";
import FogOverlay from "./FogOverlay";
import CloudOverlay from "./CloudOverlay";
import SunOverlay from "./SunOverlay";
import WeatherCanvas from "./WeatherCanvas";
import CityToggle from "./CityToggle";
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
      <CityToggle />
      <WeatherEffect category={category} />

      {/* Hero text + biking score */}
      <div className="relative min-h-screen flex flex-col">
        <WeatherHero weather={weather} isLoading={weatherLoading} />

        <div className="flex-1" />

        {/* Biking score positioned above silhouette */}
        <div className="flex justify-center pb-[22vh]">
          <BikingScore
            score={insights?.biking_score}
            isLoading={insightsLoading}
          />
        </div>

        <CitySilhouette city={city} />
      </div>
    </SkyGradient>
  );
}
