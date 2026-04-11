"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { useStationGrowth } from "@/hooks/useAnalytics";
import { CHART_COLORS, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";
import { formatNumber } from "@/lib/format";

interface Props {
  city: string;
}

export default function StationGrowthChart({ city }: Props) {
  const { data, isLoading } = useStationGrowth(city);

  const chartData = (data ?? []).map((row) => ({
    year: String(row.year),
    stations: row.station_count,
  }));

  return (
    <ChartContainer
      title="Station Growth"
      subtitle="Number of active stations by year"
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={chartData}>
          <XAxis
            dataKey="year"
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={{ stroke: CHART_THEME.grid }}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => formatNumber(v)}
            width={50}
          />
          <Tooltip
            contentStyle={{
              background: CHART_THEME.tooltipBg,
              border: `1px solid ${CHART_THEME.tooltipBorder}`,
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            formatter={(value: unknown) => {
              if (typeof value !== "number") return [String(value), "Stations"];
              return [formatNumber(value), "Stations"];
            }}
          />
          <Bar
            dataKey="stations"
            fill={CHART_COLORS.teal}
            radius={[4, 4, 0, 0]}
            fillOpacity={0.7}
          />
        </BarChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}
