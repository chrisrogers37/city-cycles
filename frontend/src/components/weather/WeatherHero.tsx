"use client";

import type { CurrentWeatherResponse } from "@/lib/types";

const CITY_NAMES: Record<string, string> = {
  nyc: "New York City",
  london: "London",
};

interface Props {
  weather: CurrentWeatherResponse | undefined;
  isLoading: boolean;
}

export default function WeatherHero({ weather, isLoading }: Props) {
  if (isLoading || !weather) {
    return (
      <div className="flex flex-col items-center justify-center pt-[30vh]">
        <div className="text-6xl font-extralight text-white/40 animate-pulse">--°</div>
        <div className="text-lg uppercase tracking-[0.15em] text-white/30 mt-2">
          Loading weather...
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center pt-[30vh] text-center px-4">
      <div
        className="text-[5rem] leading-none font-extralight text-white"
        style={{ textShadow: "0 2px 20px rgba(0,0,0,0.3)" }}
      >
        {Math.round(weather.temperature_c)}°
      </div>
      <div
        className="text-xl uppercase tracking-[0.15em] text-white/85 mt-3 font-light"
        style={{ textShadow: "0 1px 10px rgba(0,0,0,0.3)" }}
      >
        {weather.weather_description}
      </div>
      <div className="text-sm text-white/50 mt-2 tracking-wide">
        {CITY_NAMES[weather.city] ?? weather.city}
      </div>
    </div>
  );
}
