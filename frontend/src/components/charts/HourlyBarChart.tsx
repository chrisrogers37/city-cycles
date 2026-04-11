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
import { useHourlyPatterns } from "@/hooks/useAnalytics";
import { CHART_COLORS, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";
import { formatHour12, formatNumber } from "@/lib/format";

function formatHourShort(h: number): string {
  if (h === 0) return "12a";
  if (h < 12) return `${h}a`;
  if (h === 12) return "12p";
  return `${h - 12}p`;
}

function getCurrentHour(city: string): number {
  const tz = city === "london" ? "Europe/London" : "America/New_York";
  return parseInt(
    new Date().toLocaleString("en-US", { timeZone: tz, hour: "numeric", hour12: false }),
    10,
  );
}

interface Props {
  city: string;
}

export default function HourlyBarChart({ city }: Props) {
  const { data, isLoading } = useHourlyPatterns(city);
  const currentHour = getCurrentHour(city);

  const chartData = (data ?? []).map((row) => ({
    hour: row.hour_of_day,
    label: formatHourShort(row.hour_of_day),
    rides: Math.round(row.ride_count),
    isCurrent: row.hour_of_day === currentHour,
  }));

  return (
    <ChartContainer
      title="Time of Day"
      subtitle="Average rides by hour"
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={chartData} layout="horizontal">
          <XAxis
            dataKey="label"
            tick={{ fill: CHART_THEME.text, fontSize: 10 }}
            axisLine={{ stroke: CHART_THEME.grid }}
            tickLine={false}
            interval={2}
          />
          <YAxis
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => formatNumber(v)}
            width={45}
          />
          <Tooltip
            contentStyle={{
              background: CHART_THEME.tooltipBg,
              border: `1px solid ${CHART_THEME.tooltipBorder}`,
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            labelFormatter={(label) => {
              const idx = chartData.findIndex((d) => d.label === String(label));
              return idx >= 0 ? formatHour12(chartData[idx].hour) : String(label);
            }}
            formatter={(value) => {
              if (typeof value !== "number") return [String(value ?? ""), "Rides"];
              return [formatNumber(value), "Rides"];
            }}
          />
          <Bar dataKey="rides" radius={[2, 2, 0, 0]}>
            {chartData.map((entry, i) => (
              <Cell
                key={i}
                fill={entry.isCurrent ? CHART_COLORS.primary : "rgba(93,173,226,0.4)"}
              />
            ))}
          </Bar>
          <ReferenceLine
            x={formatHourShort(currentHour)}
            stroke="rgba(255,255,255,0.3)"
            strokeDasharray="3 3"
            label={{ value: "Now", position: "top", fill: "rgba(255,255,255,0.4)", fontSize: 10 }}
          />
        </BarChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}
