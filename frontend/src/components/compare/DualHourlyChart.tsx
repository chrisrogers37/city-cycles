"use client";

import { useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Legend,
} from "recharts";
import { useSimilarDayHourly } from "@/hooks/useSimilarDay";
import { formatHour12 } from "@/lib/format";

const NYC_COLOR = "#5DADE2";
const LONDON_COLOR = "#E74C3C";

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

type ViewMode = "absolute" | "normalized";

export default function DualHourlyChart() {
  const [mode, setMode] = useState<ViewMode>("absolute");
  const { data: nycData, isLoading: nycLoading } = useSimilarDayHourly("nyc");
  const { data: lonData, isLoading: lonLoading } = useSimilarDayHourly("london");

  const isLoading = nycLoading || lonLoading;

  if (isLoading) {
    return (
      <div className="glass-card p-6 animate-pulse">
        <div className="h-48 bg-white/5 rounded" />
      </div>
    );
  }

  if (!nycData && !lonData) {
    return (
      <div className="glass-card p-6">
        <p className="text-[var(--color-text-muted)] text-sm">
          Hourly pattern data is being prepared.
        </p>
      </div>
    );
  }

  // Build merged chart data for hours 0-23
  const nycTotal = nycData
    ? nycData.hours.reduce((s, h) => s + h.similar_day_avg_rides, 0)
    : 1;
  const lonTotal = lonData
    ? lonData.hours.reduce((s, h) => s + h.similar_day_avg_rides, 0)
    : 1;
  const nycOverallTotal = nycData
    ? nycData.hours.reduce((s, h) => s + (h.overall_avg_rides ?? 0), 0)
    : 1;
  const lonOverallTotal = lonData
    ? lonData.hours.reduce((s, h) => s + (h.overall_avg_rides ?? 0), 0)
    : 1;

  const chartData = Array.from({ length: 24 }, (_, hour) => {
    const nycH = nycData?.hours.find((h) => h.hour_of_day === hour);
    const lonH = lonData?.hours.find((h) => h.hour_of_day === hour);

    if (mode === "normalized") {
      return {
        hour,
        label: formatHourShort(hour),
        nycSimilar: nycH ? (nycH.similar_day_avg_rides / nycTotal) * 100 : null,
        nycOverall: nycH?.overall_avg_rides != null ? (nycH.overall_avg_rides / nycOverallTotal) * 100 : null,
        lonSimilar: lonH ? (lonH.similar_day_avg_rides / lonTotal) * 100 : null,
        lonOverall: lonH?.overall_avg_rides != null ? (lonH.overall_avg_rides / lonOverallTotal) * 100 : null,
      };
    }

    return {
      hour,
      label: formatHourShort(hour),
      nycSimilar: nycH ? Math.round(nycH.similar_day_avg_rides) : null,
      nycOverall: nycH?.overall_avg_rides != null ? Math.round(nycH.overall_avg_rides) : null,
      lonSimilar: lonH ? Math.round(lonH.similar_day_avg_rides) : null,
      lonOverall: lonH?.overall_avg_rides != null ? Math.round(lonH.overall_avg_rides) : null,
    };
  });

  const nycNowHour = getCurrentHour("nyc");
  const lonNowHour = getCurrentHour("london");

  const yAxisLabel = mode === "normalized" ? "% of daily total" : "avg rides";

  return (
    <div className="glass-card p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)]">
          Hourly Ride Pattern
        </h3>
        <div className="flex rounded-full overflow-hidden border border-white/10 text-xs">
          <button
            onClick={() => setMode("absolute")}
            className="px-3 py-1 transition-colors"
            style={{
              background: mode === "absolute" ? "rgba(255,255,255,0.1)" : "transparent",
              color: mode === "absolute" ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.4)",
            }}
          >
            Absolute
          </button>
          <button
            onClick={() => setMode("normalized")}
            className="px-3 py-1 transition-colors"
            style={{
              background: mode === "normalized" ? "rgba(255,255,255,0.1)" : "transparent",
              color: mode === "normalized" ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.4)",
            }}
          >
            % of Day
          </button>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={240}>
        <LineChart data={chartData}>
          <XAxis
            dataKey="label"
            tick={{ fill: "rgba(255,255,255,0.4)", fontSize: 11 }}
            axisLine={{ stroke: "rgba(255,255,255,0.08)" }}
            tickLine={false}
            interval={3}
          />
          <YAxis
            tick={{ fill: "rgba(255,255,255,0.3)", fontSize: 10 }}
            axisLine={false}
            tickLine={false}
            width={45}
            label={{
              value: yAxisLabel,
              angle: -90,
              position: "insideLeft",
              style: { fill: "rgba(255,255,255,0.25)", fontSize: 10 },
            }}
            tickFormatter={mode === "normalized" ? (v: number) => `${v.toFixed(0)}%` : undefined}
          />
          <Tooltip
            contentStyle={{
              background: "rgba(20,25,35,0.9)",
              border: "1px solid rgba(255,255,255,0.1)",
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            labelFormatter={(label) => `${label}`}
            formatter={(value, name) => {
              if (value == null || typeof value !== "number") return [String(value), String(name)];
              const suffix = mode === "normalized" ? "%" : " rides";
              const formatted =
                mode === "normalized"
                  ? `${value.toFixed(1)}${suffix}`
                  : `${Math.round(value).toLocaleString()}${suffix}`;
              return [formatted, String(name)];
            }}
          />
          {/* NYC overall (dashed) */}
          {nycData && (
            <Line
              type="monotone"
              dataKey="nycOverall"
              name="NYC overall"
              stroke={`${NYC_COLOR}59`}
              strokeDasharray="4 4"
              strokeWidth={1}
              dot={false}
              connectNulls
            />
          )}
          {/* London overall (dashed) */}
          {lonData && (
            <Line
              type="monotone"
              dataKey="lonOverall"
              name="London overall"
              stroke={`${LONDON_COLOR}59`}
              strokeDasharray="4 4"
              strokeWidth={1}
              dot={false}
              connectNulls
            />
          )}
          {/* NYC similar day (solid) */}
          {nycData && (
            <Line
              type="monotone"
              dataKey="nycSimilar"
              name="NYC days like today"
              stroke={NYC_COLOR}
              strokeWidth={2.5}
              dot={false}
              activeDot={{ r: 3, fill: NYC_COLOR }}
              connectNulls
            />
          )}
          {/* London similar day (solid) */}
          {lonData && (
            <Line
              type="monotone"
              dataKey="lonSimilar"
              name="London days like today"
              stroke={LONDON_COLOR}
              strokeWidth={2.5}
              dot={false}
              activeDot={{ r: 3, fill: LONDON_COLOR }}
              connectNulls
            />
          )}
          {/* NYC "Now" marker */}
          {nycData && (
            <ReferenceLine
              x={formatHourShort(nycNowHour)}
              stroke={`${NYC_COLOR}66`}
              strokeDasharray="3 3"
              label={{
                value: "NYC",
                position: "top",
                fill: `${NYC_COLOR}99`,
                fontSize: 9,
              }}
            />
          )}
          {/* London "Now" marker */}
          {lonData && (
            <ReferenceLine
              x={formatHourShort(lonNowHour)}
              stroke={`${LONDON_COLOR}66`}
              strokeDasharray="3 3"
              label={{
                value: "LON",
                position: "top",
                fill: `${LONDON_COLOR}99`,
                fontSize: 9,
              }}
            />
          )}
          <Legend
            verticalAlign="bottom"
            height={36}
            iconSize={10}
            wrapperStyle={{ fontSize: 11, color: "rgba(255,255,255,0.5)" }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
