"use client";

import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import TemperatureRidesChart from "@/components/charts/TemperatureRidesChart";
import PrecipitationChart from "@/components/charts/PrecipitationChart";
import WeatherConditionChart from "@/components/charts/WeatherConditionChart";
import HourlyWeatherImpactChart from "@/components/charts/HourlyWeatherImpactChart";
import { useCityStore } from "@/store/useCityStore";

export default function WeatherPage() {
  const city = useCityStore((s) => s.city);

  return (
    <>
      <NavBar />
      <div className="pt-20">
        <PageShell>
          <h1 className="text-2xl font-light text-white tracking-wide">Weather Deep Dive</h1>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <TemperatureRidesChart city={city} />
            <PrecipitationChart city={city} />
          </div>

          <WeatherConditionChart city={city} />
          <HourlyWeatherImpactChart city={city} />
        </PageShell>
      </div>
    </>
  );
}
