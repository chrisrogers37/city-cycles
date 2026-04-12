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
import { useWeatherCorrelationPrecip } from "@/hooks/useAnalytics";
import { useInsights } from "@/hooks/useInsights";
import { CHART_COLORS, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";
import { formatNumber } from "@/lib/format";

interface Props {
  city: string;
}

/** Map API precip_category to classified precipitation_intensity for highlighting. */
function matchesPrecip(category: string, intensity: string | null): boolean {
  if (!intensity) return false;
  const cat = category.toLowerCase();
  const int = intensity.toLowerCase();
  if (int === "none" && cat.includes("dry")) return true;
  if (int === "light" && cat.includes("light")) return true;
  if (int === "moderate" && cat.includes("moderate")) return true;
  if (int === "heavy" && cat.includes("heavy")) return true;
  return false;
}

export default function PrecipitationChart({ city }: Props) {
  const chartHeight = useIsMobile() ? 180 : 220;
  const { data, isLoading } = useWeatherCorrelationPrecip(city);
  const { data: insights } = useInsights(city);
  const todayPrecip = insights?.classified?.precipitation_intensity ?? null;

  const chartData = (data ?? []).map((row) => ({
    category: row.precip_category,
    rides: Math.round(row.avg_rides),
    days: row.days_observed,
    isToday: matchesPrecip(row.precip_category, todayPrecip),
  }));

  return (
    <ChartContainer
      title="Precipitation Impact"
      subtitle={todayPrecip ? `Today: ${todayPrecip}` : "Average daily rides by precipitation level"}
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={chartHeight}>
        <BarChart data={chartData}>
          <XAxis
            dataKey="category"
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
