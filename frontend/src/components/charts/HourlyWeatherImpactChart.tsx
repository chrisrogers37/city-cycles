"use client";

import { useIsMobile } from "@/hooks/useIsMobile";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { useHourlyWeatherImpact } from "@/hooks/useAnalytics";
import { CHART_COLORS, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";
import { formatHour12 } from "@/lib/format";

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

export default function HourlyWeatherImpactChart({ city }: Props) {
  const chartHeight = useIsMobile() ? 180 : 240;
  const { data, isLoading } = useHourlyWeatherImpact(city);
  const currentHour = getCurrentHour(city);

  const chartData = (data ?? []).map((row) => ({
    hour: row.hour_of_day,
    label: formatHourShort(row.hour_of_day),
    rain: row.rain_impact != null ? Math.round(row.rain_impact * 10) / 10 : null,
    snow: row.snow_impact != null ? Math.round(row.snow_impact * 10) / 10 : null,
    fog: row.fog_impact != null ? Math.round(row.fog_impact * 10) / 10 : null,
  }));

  return (
    <ChartContainer
      title="Hourly Weather Impact"
      subtitle="% change in rides vs clear, by hour"
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={chartHeight}>
        <LineChart data={chartData}>
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
            tickFormatter={(v: number) => `${v}%`}
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
            formatter={(value, name) => {
              if (value == null || typeof value !== "number") return ["—", String(name ?? "")];
              return [`${value > 0 ? "+" : ""}${value.toFixed(1)}%`, String(name ?? "")];
            }}
          />
          <ReferenceLine y={0} stroke="rgba(255,255,255,0.1)" />
          <Line
            type="monotone"
            dataKey="rain"
            name="Rain"
            stroke={CHART_COLORS.primary}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3 }}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="snow"
            name="Snow"
            stroke={CHART_COLORS.purple}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3 }}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="fog"
            name="Fog"
            stroke={CHART_COLORS.warning}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3 }}
            connectNulls
          />
          <ReferenceLine
            x={formatHourShort(currentHour)}
            stroke="rgba(255,255,255,0.3)"
            strokeDasharray="3 3"
            label={{ value: "Now", position: "top", fill: "rgba(255,255,255,0.4)", fontSize: 10 }}
          />
        </LineChart>
      </ResponsiveContainer>

      <div className="flex gap-4 mt-3 text-xs text-white/50">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: CHART_COLORS.primary }} />
          Rain
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: CHART_COLORS.purple }} />
          Snow
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: CHART_COLORS.warning }} />
          Fog
        </span>
      </div>
    </ChartContainer>
  );
}
