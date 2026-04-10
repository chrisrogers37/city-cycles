"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { useSimilarDayHourly } from "@/hooks/useSimilarDay";
import { useCityStore } from "@/store/useCityStore";

function formatHour(h: number): string {
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

export default function HourlyPatternChart() {
  const city = useCityStore((s) => s.city);
  const { data, isLoading } = useSimilarDayHourly(city);

  if (isLoading) {
    return (
      <div className="glass-card p-6 animate-pulse">
        <div className="h-48 bg-white/5 rounded" />
      </div>
    );
  }

  if (!data || data.hours.length === 0) {
    return (
      <div className="glass-card p-6">
        <p className="text-[var(--color-text-muted)] text-sm">
          Hourly pattern data is being prepared.
        </p>
      </div>
    );
  }

  const chartData = data.hours.map((h) => ({
    hour: h.hour_of_day,
    label: formatHour(h.hour_of_day),
    similarDay: Math.round(h.similar_day_avg_rides),
    overall: h.overall_avg_rides ? Math.round(h.overall_avg_rides) : null,
  }));

  const currentHour = getCurrentHour(city);

  return (
    <div className="glass-card p-6">
      <h3 className="text-xs uppercase tracking-wider text-[var(--color-text-muted)] mb-4">
        Hourly Ride Pattern
      </h3>
      <ResponsiveContainer width="100%" height={200}>
        <AreaChart data={chartData}>
          <defs>
            <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#5DADE2" stopOpacity={0.3} />
              <stop offset="100%" stopColor="#5DADE2" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis
            dataKey="label"
            tick={{ fill: "rgba(255,255,255,0.4)", fontSize: 11 }}
            axisLine={{ stroke: "rgba(255,255,255,0.08)" }}
            tickLine={false}
            interval={3}
          />
          <YAxis hide />
          <Tooltip
            contentStyle={{
              background: "rgba(20,25,35,0.9)",
              border: "1px solid rgba(255,255,255,0.1)",
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            labelFormatter={(label) => `${label}`}
          />
          {/* Overall average as dashed line */}
          <Area
            type="monotone"
            dataKey="overall"
            stroke="rgba(255,255,255,0.25)"
            strokeDasharray="4 4"
            fill="none"
            strokeWidth={1.5}
          />
          {/* Similar day as filled area */}
          <Area
            type="monotone"
            dataKey="similarDay"
            stroke="#5DADE2"
            fill="url(#areaGrad)"
            strokeWidth={2}
          />
          {/* Current hour marker */}
          <ReferenceLine
            x={formatHour(currentHour)}
            stroke="rgba(255,255,255,0.4)"
            strokeDasharray="3 3"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
