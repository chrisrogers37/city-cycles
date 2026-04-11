"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
} from "recharts";
import { useWeatherImpact } from "@/hooks/useAnalytics";
import { useInsights } from "@/hooks/useInsights";
import { pctChangeColor, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";

interface Props {
  city: string;
}

export default function WeatherConditionChart({ city }: Props) {
  const { data, isLoading } = useWeatherImpact(city);
  const { data: insights } = useInsights(city);
  const todayCondition = insights?.classified?.weather_category ?? null;

  const chartData = (data ?? [])
    .map((row) => ({
      condition: row.weather_condition,
      pctChange: Math.round(row.pct_change * 10) / 10,
      rides: Math.round(row.avg_rides),
      observations: row.total_observations,
      isToday: todayCondition !== null &&
        row.weather_condition.toLowerCase() === todayCondition.toLowerCase(),
    }))
    .sort((a, b) => b.pctChange - a.pctChange);

  return (
    <ChartContainer
      title="Weather Condition Impact"
      subtitle={todayCondition ? `Today: ${todayCondition} — % change vs clear` : "% change in rides vs clear conditions"}
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={chartData} layout="vertical">
          <XAxis
            type="number"
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={{ stroke: CHART_THEME.grid }}
            tickLine={false}
            tickFormatter={(v: number) => `${v}%`}
          />
          <YAxis
            type="category"
            dataKey="condition"
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            width={80}
          />
          <Tooltip
            contentStyle={{
              background: CHART_THEME.tooltipBg,
              border: `1px solid ${CHART_THEME.tooltipBorder}`,
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            formatter={(value: unknown, _name: string, props: { payload?: { observations?: number; rides?: number } }) => {
              if (typeof value !== "number") return [String(value), "Impact"];
              const { observations, rides } = props.payload ?? {};
              return [`${value > 0 ? "+" : ""}${value.toFixed(1)}% (${rides ?? "?"} avg rides, ${observations ?? "?"} obs)`, "vs Clear"];
            }}
          />
          <ReferenceLine x={0} stroke="rgba(255,255,255,0.15)" />
          <Bar dataKey="pctChange" radius={[0, 4, 4, 0]}>
            {chartData.map((entry, i) => (
              <Cell
                key={i}
                fill={pctChangeColor(entry.pctChange)}
                fillOpacity={entry.isToday ? 1 : 0.6}
                stroke={entry.isToday ? "#fff" : "none"}
                strokeWidth={entry.isToday ? 1 : 0}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}
