"use client";

import { useIsMobile } from "@/hooks/useIsMobile";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { useWeatherCorrelationTemp } from "@/hooks/useAnalytics";
import { useInsights } from "@/hooks/useInsights";
import { CHART_COLORS, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";
import { formatNumber } from "@/lib/format";

interface Props {
  city: string;
}

export default function TemperatureRidesChart({ city }: Props) {
  const chartHeight = useIsMobile() ? 180 : 240;
  const { data, isLoading } = useWeatherCorrelationTemp(city);
  const { data: insights } = useInsights(city);
  const todayBand = insights?.classified?.temperature_band ?? null;

  const chartData = (data ?? []).map((row) => ({
    range: row.temp_range,
    rides: Math.round(row.avg_rides),
    days: row.days_observed,
    isToday: todayBand !== null && row.temp_range.toLowerCase().includes(todayBand.toLowerCase()),
  }));

  return (
    <ChartContainer
      title="Temperature vs Rides"
      subtitle={todayBand ? `Today: ${todayBand}` : "Average daily rides by temperature range"}
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={chartHeight}>
        <BarChart data={chartData}>
          <XAxis
            dataKey="range"
            tick={{ fill: CHART_THEME.text, fontSize: 10 }}
            axisLine={{ stroke: CHART_THEME.grid }}
            tickLine={false}
            angle={-25}
            textAnchor="end"
            height={50}
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
            formatter={(value, _name, props) => {
              if (typeof value !== "number") return [String(value ?? ""), "Avg Rides"];
              const days = (props as { payload?: { days?: number } }).payload?.days;
              return [`${formatNumber(value)} rides (${days ?? "?"} days)`, "Avg Daily"];
            }}
          />
          <Bar dataKey="rides" radius={[4, 4, 0, 0]}>
            {chartData.map((entry, i) => (
              <Cell
                key={i}
                fill={entry.isToday ? CHART_COLORS.warning : CHART_COLORS.primary}
                fillOpacity={entry.isToday ? 1 : 0.5}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}
