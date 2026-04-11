"use client";

import { useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { useDurationTrends } from "@/hooks/useAnalytics";
import { COLORWAY, CHART_THEME } from "@/components/charts/chart-theme";
import ChartContainer from "@/components/ui/ChartContainer";
import { monthName } from "@/lib/format";

interface Props {
  city: string;
  startDate: string;
  endDate: string;
}

export default function DurationTrendChart({ city, startDate, endDate }: Props) {
  const { data, isLoading } = useDurationTrends(city, startDate, endDate);

  const { chartData, years } = useMemo(() => {
    if (!data || data.length === 0) return { chartData: [], years: [] };

    const yearSet = new Set<number>();
    const byMonth = new Map<number, Record<string, number>>();

    for (const row of data) {
      if (row.avg_duration == null) continue;
      yearSet.add(row.year);
      const entry = byMonth.get(row.month) ?? { month: row.month };
      entry[String(row.year)] = Math.round(row.avg_duration * 10) / 10;
      byMonth.set(row.month, entry);
    }

    const yrs = Array.from(yearSet).sort();
    const cd = Array.from(byMonth.values()).sort(
      (a, b) => (a.month as number) - (b.month as number),
    );
    return { chartData: cd, years: yrs };
  }, [data]);

  const currentMonth = new Date().getMonth() + 1;

  return (
    <ChartContainer
      title="Duration Trends"
      subtitle="Average ride duration (minutes) per month"
      isLoading={isLoading}
      isEmpty={chartData.length === 0}
    >
      <ResponsiveContainer width="100%" height={240}>
        <LineChart data={chartData}>
          <XAxis
            dataKey="month"
            tickFormatter={(m: number) => monthName(m).slice(0, 3)}
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={{ stroke: CHART_THEME.grid }}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: CHART_THEME.text, fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v: number) => `${v}m`}
            width={40}
          />
          <Tooltip
            contentStyle={{
              background: CHART_THEME.tooltipBg,
              border: `1px solid ${CHART_THEME.tooltipBorder}`,
              borderRadius: 8,
              color: "#fff",
              fontSize: 12,
            }}
            labelFormatter={(m: number) => monthName(m)}
            formatter={(value: unknown, name: string) => {
              if (typeof value !== "number") return [String(value), name];
              return [`${value.toFixed(1)} min`, name];
            }}
          />
          {years.map((year, i) => (
            <Line
              key={year}
              type="monotone"
              dataKey={String(year)}
              stroke={COLORWAY[i % COLORWAY.length]}
              strokeWidth={year === years[years.length - 1] ? 2.5 : 1.5}
              strokeOpacity={year === years[years.length - 1] ? 1 : 0.5}
              dot={false}
              activeDot={{ r: 3 }}
            />
          ))}
          <ReferenceLine
            x={currentMonth}
            stroke="rgba(255,255,255,0.3)"
            strokeDasharray="3 3"
            label={{ value: "Now", position: "top", fill: "rgba(255,255,255,0.4)", fontSize: 10 }}
          />
        </LineChart>
      </ResponsiveContainer>

      <div className="flex flex-wrap gap-3 mt-3 text-xs text-white/50">
        {years.map((year, i) => (
          <span key={year} className="flex items-center gap-1.5">
            <span
              className="inline-block w-3 h-0.5 rounded"
              style={{
                background: COLORWAY[i % COLORWAY.length],
                opacity: year === years[years.length - 1] ? 1 : 0.5,
              }}
            />
            {year}
          </span>
        ))}
      </div>
    </ChartContainer>
  );
}
