"use client";

import { useState } from "react";
import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import MetricCard from "@/components/ui/MetricCard";
import DatePresetBar from "@/components/ui/DatePresetBar";
import MonthlyTrendChart from "@/components/charts/MonthlyTrendChart";
import DurationTrendChart from "@/components/charts/DurationTrendChart";
import HourlyBarChart from "@/components/charts/HourlyBarChart";
import MemberPercentageChart from "@/components/charts/MemberPercentageChart";
import StationGrowthChart from "@/components/charts/StationGrowthChart";
import { useDailyMetrics } from "@/hooks/useAnalytics";
import { useCityStore } from "@/store/useCityStore";
import CityToggle from "@/components/weather/CityToggle";
import { formatNumber, formatDuration } from "@/lib/format";

const DEFAULT_START = "2015-01-01";
const DEFAULT_END = `${new Date().getFullYear()}-12-31`;

export default function AnalyticsPage() {
  const city = useCityStore((s) => s.city);
  const [startDate, setStartDate] = useState(DEFAULT_START);
  const [endDate, setEndDate] = useState(DEFAULT_END);
  const { data: metrics, isLoading } = useDailyMetrics(city, startDate, endDate);

  return (
    <>
      <NavBar alwaysVisible />
      <div className="pt-20">
        <PageShell>
          <div className="flex items-center justify-between flex-wrap gap-4">
            <h1 className="text-2xl font-light text-white tracking-wide">Ride Analytics</h1>
            <CityToggle />
          </div>

          <DatePresetBar
            startDate={startDate}
            endDate={endDate}
            onChange={(s, e) => { setStartDate(s); setEndDate(e); }}
          />

          {/* Key Metrics */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <MetricCard
              label="Total Rides"
              value={isLoading ? "..." : metrics ? formatNumber(metrics.total_rides) : "—"}
            />
            <MetricCard
              label="Average Daily"
              value={isLoading ? "..." : metrics ? formatNumber(metrics.avg_daily_rides) : "—"}
            />
            <MetricCard
              label="Average Duration"
              value={
                isLoading
                  ? "..."
                  : metrics?.avg_ride_duration_minutes
                    ? formatDuration(metrics.avg_ride_duration_minutes)
                    : "—"
              }
            />
          </div>

          <MonthlyTrendChart city={city} startDate={startDate} endDate={endDate} />
          <DurationTrendChart city={city} startDate={startDate} endDate={endDate} />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <HourlyBarChart city={city} />
            <StationGrowthChart city={city} />
          </div>

          <MemberPercentageChart city={city} startDate={startDate} endDate={endDate} />
        </PageShell>
      </div>
    </>
  );
}
