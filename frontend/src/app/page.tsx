"use client";

import WeatherScene from "@/components/weather/WeatherScene";
import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import SimilarDayCard from "@/components/insights/SimilarDayCard";
import HourlyPatternChart from "@/components/insights/HourlyPatternChart";
import InsightCards from "@/components/insights/InsightCards";
import ForecastStrip from "@/components/insights/ForecastStrip";

export default function LandingPage() {
  return (
    <>
      <NavBar />
      <WeatherScene />
      <PageShell>
        <SimilarDayCard />
        <HourlyPatternChart />
        <InsightCards />
        <ForecastStrip />
      </PageShell>
    </>
  );
}
