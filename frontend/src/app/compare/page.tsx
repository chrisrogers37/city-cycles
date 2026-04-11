"use client";

import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import DualWeatherHeader from "@/components/compare/DualWeatherHeader";
import CrossCityInsight from "@/components/compare/CrossCityInsight";
import ComparisonStatsTable from "@/components/compare/ComparisonStatsTable";
import DualHourlyChart from "@/components/compare/DualHourlyChart";

export default function ComparePage() {
  return (
    <>
      <NavBar alwaysVisible />
      <div className="pt-20">
        <PageShell>
          <h1 className="text-2xl font-light text-white tracking-wide">NYC vs London</h1>
          <DualWeatherHeader />
          <CrossCityInsight />
          <ComparisonStatsTable />
          <DualHourlyChart />
        </PageShell>
      </div>
    </>
  );
}
