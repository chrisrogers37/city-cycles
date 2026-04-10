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
import { useSimilarDayHourly, useSimilarDay } from "@/hooks/useSimilarDay";
import { useCityStore } from "@/store/useCityStore";
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

export default function HourlyPatternChart() {
  const city = useCityStore((s) => s.city);
  const { data, isLoading } = useSimilarDayHourly(city);
  const { data: dailyData } = useSimilarDay(city);

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
    label: formatHourShort(h.hour_of_day),
    similarDay: Math.round(h.similar_day_avg_rides),
    overall: h.overall_avg_rides ? Math.round(h.overall_avg_rides) : null,
  }));

  const currentHour = getCurrentHour(city);

  // Peak hour from daily stats
  const peakHour = dailyData?.peak_hour_start ?? null;
  const peakEnd = dailyData?.peak_hour_end ?? null;
  const peakLabel =
    peakHour !== null && peakEnd !== null
      ? `Peak ${formatHour12(peakHour)}–${formatHour12(peakEnd)}`
      : null;

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
          {/* Overall average as dashed line — draws in from left */}
          <Area
            type="monotone"
            dataKey="overall"
            stroke="rgba(255,255,255,0.25)"
            strokeDasharray="4 4"
            fill="none"
            strokeWidth={1.5}
            activeDot={false}
            isAnimationActive={true}
            animationDuration={1400}
            animationEasing="ease-out"
          />
          {/* Similar day as filled area — draws in from left */}
          <Area
            type="monotone"
            dataKey="similarDay"
            stroke="#5DADE2"
            fill="url(#areaGrad)"
            strokeWidth={2}
            activeDot={{ r: 4, fill: "#5DADE2" }}
            isAnimationActive={true}
            animationDuration={1200}
            animationEasing="ease-out"
          />
          {/* Current hour marker */}
          <ReferenceLine
            x={formatHourShort(currentHour)}
            stroke="rgba(255,255,255,0.4)"
            strokeDasharray="3 3"
            label={{
              value: "Now",
              position: "top",
              fill: "rgba(255,255,255,0.5)",
              fontSize: 10,
            }}
          />
          {/* Peak hour marker */}
          {peakHour !== null && (
            <ReferenceLine
              x={formatHourShort(peakHour)}
              stroke="rgba(46,204,113,0.4)"
              strokeDasharray="3 3"
              label={{
                value: peakLabel ?? "Peak",
                position: "top",
                fill: "rgba(46,204,113,0.7)",
                fontSize: 10,
              }}
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
      {/* Legend */}
      <div className="flex gap-4 mt-3 text-xs text-[var(--color-text-muted)]">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 bg-[#5DADE2] rounded" />
          Days like today
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 border-t border-dashed border-white/25" />
          Overall average
        </span>
      </div>
    </div>
  );
}
