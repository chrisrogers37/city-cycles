"use client";

import WeatherScene from "@/components/weather/WeatherScene";
import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import SimilarDayCard from "@/components/insights/SimilarDayCard";
import HourlyPatternChart from "@/components/insights/HourlyPatternChart";
import DurationInsight from "@/components/insights/DurationInsight";
import MemberCasualSplit from "@/components/insights/MemberCasualSplit";
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
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <DurationInsight />
          <MemberCasualSplit />
        </div>
        <InsightCards />
        <ForecastStrip />
      </PageShell>
    </>
  );
}
